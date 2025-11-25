"""Bitquery GraphQL client for querying Polymarket CTF Exchange events."""
import requests
from typing import Dict, List, Optional
from config import Config

class BitqueryClient:
    """Client for querying Bitquery GraphQL API."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or Config.OAUTH_TOKEN
        self.api_url = Config.BITQUERY_API_URL
        self.headers = {
            "Content-Type": "application/json"
        }
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"
    
    def _execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute a GraphQL query with a sane default timeout."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        
        response = requests.post(
            self.api_url,
            json=payload,
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    def get_order_filled_events(
        self,
        limit: int = 20,
        asset_ids: Optional[List[str]] = None,
        trader_address: Optional[str] = None,
        since_hours: Optional[int] = None
    ) -> List[Dict]:
        """Query OrderFilled events from CTF Exchange."""
        # Note: We don't filter by asset_ids or trader_address in the query because
        # these values are in the event arguments (makerAssetId/takerAssetId for assets,
        # maker/taker for traders), not in easily queryable fields.
        # Filtering will be done in Python after parsing the events.
        # We fetch more events to account for filtering.
        
        # Time filter
        time_filter = ""
        if since_hours:
            time_filter = f'Block: {{Time: {{since_relative: {{hours_ago: {since_hours}}}}}}},'
        
        where_clause = ""
        if time_filter:
            where_clause = time_filter + ","
        
        query = f"""
        {{
          EVM(dataset: {Config.DATASET}, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                {time_filter}
                {where_clause}
                Log: {{Signature: {{Name: {{in: ["OrderFilled"]}}}}}}, 
                LogHeader: {{
                  Address: {{
                    in: [
                      "{Config.CTF_EXCHANGE_ADDRESS}",
                      "{Config.LEGACY_EXCHANGE_ADDRESS}"
                    ]
                  }}
                }}
              }}
              limit: {{count: {limit * 5 if asset_ids else limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            # Ensure we always return a list, even if the API response is malformed
            if events is None:
                return []
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching order filled events: {e}")
            return []
    
    def get_order_filled_events_by_asset_id(
        self,
        asset_id: str,
        limit: int = 100,
        since_hours: Optional[int] = None
    ) -> List[Dict]:
        """Query OrderFilled events filtered by asset ID.
        
        This method filters OrderFilled events where the asset ID appears
        as either makerAssetId or takerAssetId in the event arguments.
        
        Args:
            asset_id: The asset ID to filter by (as a string or bigInteger)
            limit: Maximum number of events to return
            since_hours: Optional time filter (hours ago)
            
        Returns:
            List of OrderFilled events matching the asset ID
        """
        # Time filter
        time_filter = ""
        if since_hours:
            time_filter = f'Block: {{Time: {{since_relative: {{hours_ago: {since_hours}}}}}}},'
        
        query = f"""
        {{
          EVM(dataset: {Config.DATASET}, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                {time_filter}
                Arguments: {{
                  includes: {{
                    Value: {{
                      BigInteger: {{
                        eq: "{asset_id}"
                      }}
                    }}
                  }}
                }},
                Log: {{Signature: {{Name: {{in: ["OrderFilled"]}}}}}}, 
                LogHeader: {{
                  Address: {{
                    in: [
                      "{Config.CTF_EXCHANGE_ADDRESS}",
                      "{Config.LEGACY_EXCHANGE_ADDRESS}"
                    ]
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            # Ensure we always return a list, even if the API response is malformed
            if events is None:
                return []
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching order filled events by asset ID: {e}")
            return []

    def get_order_filled_events_by_trader(
        self,
        trader_address: str,
        limit: int = 10000,
        since_hours: Optional[int] = None
    ) -> List[Dict]:
        """Query OrderFilled events filtered by maker or taker address."""
        if not trader_address:
            return []

        trader_address_normalized = trader_address.lower()

        # Time filter
        time_filter = ""
        if since_hours:
            time_filter = f'Block: {{Time: {{since_relative: {{hours_ago: {since_hours}}}}}}},'

        query = f"""
        {{
          EVM(dataset: {Config.DATASET}, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                {time_filter}
                Arguments: {{
                  includes: {{
                    Name: {{
                      in: ["maker", "taker"]
                    }},
                    Value: {{
                      Address: {{
                        is: "{trader_address_normalized}"
                      }}
                    }}
                  }}
                }},
                Log: {{Signature: {{Name: {{in: ["OrderFilled"]}}}}}}, 
                LogHeader: {{
                  Address: {{
                    in: [
                      "{Config.CTF_EXCHANGE_ADDRESS}",
                      "{Config.LEGACY_EXCHANGE_ADDRESS}"
                    ]
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """

        try:
            result = self._execute_query(query)

            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []

            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            if events is None:
                return []
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching order filled events by trader: {e}")
            return []
    
    def get_token_registered_events(
        self,
        limit: int = 10,
        condition_id: Optional[str] = None,
        since_days: int = 6
    ) -> List[Dict]:
        """Query TokenRegistered events."""
        condition_filter = ""
        if condition_id:
            condition_filter = f'''
                Arguments: {{
                  includes: {{
                    Name: {{is: "conditionId"}}, 
                    Value: {{Bytes: {{is: "{condition_id}"}}}}
                  }}
                }},
            '''
        
        query = f"""
        {{
          EVM(dataset: combined, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                Block: {{Time: {{since_relative: {{days_ago: {since_days}}}}}}},
                {condition_filter}
                Log: {{Signature: {{Name: {{in: ["TokenRegistered"]}}}}}}, 
                LogHeader: {{Address: {{is: "{Config.LEGACY_EXCHANGE_ADDRESS}"}}}}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
              }}
              Transaction {{
                Hash
                From
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        result = self._execute_query(query)
        events = result.get("data", {}).get("EVM", {}).get("Events", [])
        return events
    
    def get_order_matched_events(
        self,
        limit: int = 20,
        since_hours: Optional[int] = None
    ) -> List[Dict]:
        """Query OrderMatched events."""
        time_filter = ""
        if since_hours:
            time_filter = f'Block: {{Time: {{since_relative: {{hours_ago: {since_hours}}}}}}},'
        
        query = f"""
        {{
          EVM(dataset: {Config.DATASET}, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                {time_filter}
                Log: {{Signature: {{Name: {{in: ["OrdersMatched"]}}}}}}, 
                LogHeader: {{Address: {{is: "{Config.CTF_EXCHANGE_ADDRESS}"}}}}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        result = self._execute_query(query)
        events = result.get("data", {}).get("EVM", {}).get("Events", [])
        return events
    
    def get_token_registered_by_asset_id(
        self,
        asset_id: str,
        limit: int = 10,
        since_days: int = 10
    ) -> List[Dict]:
        """Query TokenRegistered events by asset ID to get conditionId.
        
        Args:
            asset_id: The asset ID to search for
            limit: Maximum number of events to return
            since_days: Number of days to look back
            
        Returns:
            List of TokenRegistered events containing the asset ID
        """
        query = f"""
        {{
          EVM(dataset: archive, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                Arguments: {{
                  includes: {{
                    Value: {{
                      String: {{
                        includesCaseInsensitive: "{asset_id}"
                      }}
                    }}
                  }}
                }},
                Log: {{
                  Signature: {{
                    Name: {{
                      includesCaseInsensitive: "tokenregistered"
                    }}
                  }}
                }},
                Block: {{
                  Time: {{
                    since_relative: {{
                      days_ago: {since_days}
                    }}
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              TransactionStatus {{
                Success
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
              Log {{
                SmartContract
                Signature {{
                  Name
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching token registered events by asset ID: {e}")
            return []
    
    def get_question_events_by_condition_id(
        self,
        condition_id: str,
        limit: int = 1,
        since_days: int = 10
    ) -> List[Dict]:
        """Query QuestionInitialized/ConditionPreparation events by conditionId to get questionId.
        
        Args:
            condition_id: The condition ID to search for (as hex string)
            limit: Maximum number of events to return
            since_days: Number of days to look back
            
        Returns:
            List of QuestionInitialized/ConditionPreparation events
        """
        query = f"""
        {{
          EVM(dataset: archive, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                Block: {{
                  Time: {{
                    since_relative: {{
                      days_ago: {since_days}
                    }}
                  }}
                }},
                Arguments: {{
                  includes: {{
                    Name: {{
                      in: ["questionID", "conditionId"]
                    }},
                    Value: {{
                      Bytes: {{
                        is: "{condition_id}"
                      }}
                    }}
                  }}
                }},
                Log: {{
                  Signature: {{
                    Name: {{
                      in: ["QuestionInitialized", "ConditionPreparation"]
                    }}
                  }}
                }},
                LogHeader: {{
                  Address: {{
                    in: [
                      "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
                      "0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"
                    ]
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              TransactionStatus {{
                Success
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching question events by condition ID: {e}")
            return []
    
    def get_question_data_by_question_id(
        self,
        question_id: str,
        limit: int = 1,
        since_days: int = 10
    ) -> List[Dict]:
        """Query question data by questionId to get ancillaryData.
        
        Args:
            question_id: The question ID to search for (as hex string, without 0x prefix)
            limit: Maximum number of events to return
            since_days: Number of days to look back
            
        Returns:
            List of events containing question data with ancillaryData
        """
        # Ensure question_id is in the correct format (hex without 0x)
        question_id_clean = question_id.replace("0x", "").lower()
        
        query = f"""
        {{
          EVM(dataset: archive, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                Block: {{
                  Time: {{
                    since_relative: {{
                      days_ago: {since_days}
                    }}
                  }}
                }},
                Arguments: {{
                  includes: {{
                    Name: {{
                      in: ["questionID"]
                    }},
                    Value: {{
                      Bytes: {{
                        is: "{question_id_clean}"
                      }}
                    }}
                  }}
                }},
                Log: {{
                  Signature: {{
                    Name: {{
                      in: ["QuestionInitialized"]
                    }}
                  }}
                }},
                LogHeader: {{
                  Address: {{
                    in: ["0x65070BE91477460D8A7AeEb94ef92fe056C2f2A7"]
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              TransactionStatus {{
                Success
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
            
            # Check for GraphQL errors
            if "errors" in result:
                error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
                print(f"GraphQL errors: {', '.join(error_messages)}")
                return []
            
            events = result.get("data", {}).get("EVM", {}).get("Events", [])
            return events if isinstance(events, list) else []
        except Exception as e:
            print(f"Error fetching question data by question ID: {e}")
            return []

    def get_recent_question_initialized_events(self, limit: int = 25) -> List[Dict]:
        """Fetch recent QuestionInitialized events for ancillaryData analysis."""
        query = f"""
        {{
          EVM(dataset: {Config.DATASET}, network: {Config.NETWORK}) {{
            Events(
              orderBy: {{descending: Block_Time}}
              where: {{
                Log: {{Signature: {{Name: {{in: ["QuestionInitialized"]}}}}}},
                LogHeader: {{
                  Address: {{
                    is: "{Config.QUESTION_CONTRACT_ADDRESS}"
                  }}
                }}
              }}
              limit: {{count: {limit}}}
            ) {{
              Block {{
                Time
                Number
                Hash
              }}
              Transaction {{
                Hash
                From
                To
              }}
              TransactionStatus {{
                Success
              }}
              Arguments {{
                Name
                Value {{
                  ... on EVM_ABI_Integer_Value_Arg {{
                    integer
                  }}
                  ... on EVM_ABI_Address_Value_Arg {{
                    address
                  }}
                  ... on EVM_ABI_String_Value_Arg {{
                    string
                  }}
                  ... on EVM_ABI_BigInt_Value_Arg {{
                    bigInteger
                  }}
                  ... on EVM_ABI_Bytes_Value_Arg {{
                    hex
                  }}
                  ... on EVM_ABI_Boolean_Value_Arg {{
                    bool
                  }}
                }}
              }}
            }}
          }}
        }}
        """
        
        try:
            result = self._execute_query(query)
        except Exception as exc:  # pragma: no cover - network errors bubble up
            print(f"Error fetching question initialized events: {exc}")
            return []

        if "errors" in result:
            error_messages = [err.get("message", "Unknown error") for err in result["errors"]]
            print(f"GraphQL errors: {', '.join(error_messages)}")
            return []
        
        events = result.get("data", {}).get("EVM", {}).get("Events", [])
        return events if isinstance(events, list) else []

