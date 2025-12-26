"""Track and analyze positions from Polymarket trades."""
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from bitquery_client import BitqueryClient
from processing import (
    extract_value,
    extract_string_value,
    process_trade_amounts
)

@dataclass
class Position:
    """Represents a trading position."""
    asset_id: str
    trader_address: str
    maker_address: str
    taker_address: str
    amount: float
    price: float
    direction: str  # "YES" or "NO"
    timestamp: datetime
    tx_hash: str
    block_number: int
    
    def to_dict(self) -> Dict:
        """Convert position to dictionary."""
        return {
            "asset_id": self.asset_id,
            "trader_address": self.trader_address,
            "maker_address": self.maker_address,
            "taker_address": self.taker_address,
            "amount": self.amount,
            "price": self.price,
            "direction": self.direction,
            "timestamp": self.timestamp.isoformat(),
            "tx_hash": self.tx_hash,
            "block_number": self.block_number
        }

class PositionTracker:
    """Track positions from OrderFilled events."""
    
    def __init__(self, bitquery_client: BitqueryClient):
        self.client = bitquery_client
        self.positions: List[Position] = []
    
    
    def parse_order_filled_event(self, event: Dict) -> Optional[Position]:
        """Parse an OrderFilled event into a Position.
        
        Polymarket OrderFilled event structure:
        - makerAssetId: Asset ID that maker is giving (0 = USDC, otherwise = outcome token)
        - takerAssetId: Asset ID that taker is giving (0 = USDC, otherwise = outcome token)
        - makerAmount: Amount maker is giving
        - takerAmount: Amount taker is giving
        - maker: Maker address
        - taker: Taker address
        
        Price calculation: Price = USDC paid / outcome tokens received
        """
        try:
            args = event.get("Arguments", [])
            
            # Extract arguments from event
            arg_dict = {arg["Name"]: arg["Value"] for arg in args}
            
            # Extract asset IDs
            maker_asset_id = None
            taker_asset_id = None
            
            if "makerAssetId" in arg_dict:
                value = arg_dict["makerAssetId"]
                maker_asset_id_str = extract_string_value(value)
                if maker_asset_id_str:
                    maker_asset_id = maker_asset_id_str
            
            if "takerAssetId" in arg_dict:
                value = arg_dict["takerAssetId"]
                taker_asset_id_str = extract_string_value(value)
                if taker_asset_id_str:
                    taker_asset_id = taker_asset_id_str
            
            # Identify which is USDC (asset ID = "0") and which is the outcome token
            # USDC asset ID is "0" (as a string or bigInteger)
            usdc_asset_id_values = ["0", 0, "0x0"]
            outcome_asset_id = None
            
            # Check if maker asset ID is USDC
            maker_is_usdc = (maker_asset_id in usdc_asset_id_values or 
                           (maker_asset_id and str(maker_asset_id).strip() == "0"))
            
            # Check if taker asset ID is USDC
            taker_is_usdc = (taker_asset_id in usdc_asset_id_values or 
                           (taker_asset_id and str(taker_asset_id).strip() == "0"))
            
            if maker_is_usdc:
                # Maker is giving USDC, taker is giving outcome tokens
                outcome_asset_id = taker_asset_id
                usdc_given = "maker"
                tokens_received = "taker"
            elif taker_is_usdc:
                # Taker is giving USDC, maker is giving outcome tokens
                outcome_asset_id = maker_asset_id
                usdc_given = "taker"
                tokens_received = "maker"
            else:
                # Neither is USDC, or both asset IDs are missing
                # Try to use the non-zero asset ID as outcome token
                outcome_asset_id = maker_asset_id or taker_asset_id
                if not outcome_asset_id:
                    return None
                # Default assumption: taker gives USDC, maker gives tokens
                usdc_given = "taker"
                tokens_received = "maker"
            
            # Extract amounts - Polymarket uses makerAmountFilled and takerAmountFilled
            maker_amount = None
            taker_amount = None
            
            # Try the actual field names first
            for key in ["makerAmountFilled", "makerAmount", "makerFillAmount", "makerFilledAmount"]:
                if key in arg_dict:
                    maker_amount = extract_value(arg_dict[key])
                    if maker_amount is not None:
                        break
            
            for key in ["takerAmountFilled", "takerAmount", "takerFillAmount", "takerFilledAmount", "fillAmount", "amount"]:
                if key in arg_dict:
                    taker_amount = extract_value(arg_dict[key])
                    if taker_amount is not None:
                        break
            
            # Calculate USDC paid and tokens received based on which side is USDC
            usdc_paid = None
            tokens_amount = None
            
            if usdc_given == "maker":
                usdc_paid = maker_amount
                tokens_amount = taker_amount
            else:  # usdc_given == "taker"
                usdc_paid = taker_amount
                tokens_amount = maker_amount
            
            # Process trade amounts using processing module
            usdc_normalized, amount, price = process_trade_amounts(usdc_paid, tokens_amount)
            
            if usdc_paid is None and tokens_amount is None:
                # No amounts found
                return None
            
            # Extract trader addresses
            maker = None
            taker = None
            
            if "maker" in arg_dict:
                maker = extract_string_value(arg_dict["maker"])
                if maker:
                    maker = maker.lower()  # Normalize to lowercase
            if "taker" in arg_dict:
                taker = extract_string_value(arg_dict["taker"])
                if taker:
                    taker = taker.lower()  # Normalize to lowercase
            
            # Determine trader: the one receiving outcome tokens (buying)
            # If maker gives USDC, taker is buying (receiving tokens)
            # If taker gives USDC, maker is buying (receiving tokens)
            if usdc_given == "maker":
                trader_address = taker or maker  # Taker is buying
            else:
                trader_address = maker or taker  # Maker is buying
            
            # Fallback to transaction From if no maker/taker
            if not trader_address:
                tx = event.get("Transaction", {})
                trader_address = tx.get("From", "")
                if trader_address:
                    trader_address = trader_address.lower()  # Normalize to lowercase
            
            if not outcome_asset_id:
                return None
            
            # Determine direction (YES/NO) - this would need additional context
            # For now, we'll use a placeholder. In practice, you'd need to check
            # the asset ID against TokenRegistered events to determine if it's YES or NO
            direction = "YES"  # Default, should be determined from asset ID mapping
            
            block = event.get("Block", {})
            tx = event.get("Transaction", {})
            
            timestamp_str = block.get("Time", "")
            if timestamp_str:
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            else:
                timestamp = datetime.now()
            
            return Position(
                asset_id=str(outcome_asset_id),
                trader_address=trader_address or "",
                maker_address=maker or "",
                taker_address=taker or "",
                amount=amount or 0.0,
                price=price or 0.0,
                direction=direction,
                timestamp=timestamp,
                tx_hash=tx.get("Hash", ""),
                block_number=block.get("Number", 0)
            )
        except Exception as e:
            print(f"Error parsing event: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def track_trader(self, trader_address: str, limit: int = 10000) -> List[Position]:
        """Track all positions from a specific trader."""
        # Normalize address to lowercase for consistent comparison
        trader_address_normalized = trader_address.lower() if trader_address else ""
        
        events = self.client.get_order_filled_events_by_trader(
            trader_address=trader_address_normalized,
            limit=limit * 2  # Grab a few extra in case of malformed events
        ) or []
        
        positions = []
        for event in events:
            position = self.parse_order_filled_event(event)
            if position and position.trader_address.lower() == trader_address_normalized:
                positions.append(position)
                self.positions.append(position)
                # Stop once we have enough positions
                if len(positions) >= limit:
                    break
        
        return positions
    
    def follow_trader_positions(self, maker_address: str, limit: int = 10000, since_hours: Optional[int] = None) -> List[Position]:
        """Follow positions where a specific address is the maker.
        
        This method filters OrderFilled events where the maker address
        matches the provided address, allowing you to track trades where
        a specific trader is making orders.
        
        Args:
            maker_address: The maker address to filter by
            limit: Maximum number of positions to return
            since_hours: Optional time filter (hours ago)
            
        Returns:
            List of Position objects where maker address matches
        """
        # Normalize address to lowercase for consistent comparison
        maker_address_normalized = maker_address.lower() if maker_address else ""
        
        if not maker_address_normalized:
            return []
        
        events = self.client.follow_trader(
            maker_address=maker_address_normalized,
            limit=limit * 2,  # Grab a few extra in case of malformed events
            since_hours=since_hours
        ) or []
        
        positions = []
        for event in events:
            position = self.parse_order_filled_event(event)
            if position and position.maker_address.lower() == maker_address_normalized:
                positions.append(position)
                self.positions.append(position)
                # Stop once we have enough positions
                if len(positions) >= limit:
                    break
        
        return positions
    
    def get_recent_positions(self, limit: int = 20) -> List[Position]:
        """Get recent positions from all traders."""
        events = self.client.get_order_filled_events(limit=limit)
        
        print(f"PositionTracker: Received {len(events)} events from API")
        
        positions = []
        for i, event in enumerate(events):
            position = self.parse_order_filled_event(event)
            if position:
                positions.append(position)
                self.positions.append(position)
            else:
                # Debug: log why parsing failed - show all arguments
                if i < 1:  # Only log first failure to see all fields
                    args = event.get("Arguments", [])
                    print(f"PositionTracker: Failed to parse event {i}. Args count: {len(args)}")
                    if args:
                        all_arg_names = [arg.get('Name', 'N/A') for arg in args]
                        # print(f"PositionTracker: All arg names: {all_arg_names}")
                        # Print all argument values for debugging
                        # for arg in args:
                        #     name = arg.get('Name', 'N/A')
                        #     value = arg.get('Value', {})
                        #     print(f"  - {name}: {value}")
        
        print(f"PositionTracker: Successfully parsed {len(positions)} positions from {len(events)} events")
        return positions
    
    def get_positions_by_asset(self, asset_id: str) -> List[Position]:
        """Get all positions for a specific asset ID.
        
        Uses a dedicated query method that filters OrderFilled events by asset ID
        directly in the GraphQL query for better performance.
        """
        # Normalize asset_id for comparison (strip whitespace, ensure string)
        asset_id_normalized = str(asset_id).strip()
        
        # Use the new method that filters by asset ID in the query
        events = self.client.get_order_filled_events_by_asset_id(
            asset_id=asset_id_normalized,
            limit=100
        )
        
        # Handle case where events might be None
        if events is None:
            return []
        
        print(f"PositionTracker: Found {len(events)} OrderFilled events for asset ID {asset_id_normalized}")
        
        positions = []
        for event in events:
            position = self.parse_order_filled_event(event)
            if position:
                # Double-check that the asset ID matches (should already be filtered by query)
                position_asset_id = str(position.asset_id).strip()
                if position_asset_id == asset_id_normalized:
                    positions.append(position)
        
        print(f"PositionTracker: Parsed {len(positions)} positions for asset ID {asset_id_normalized}")
        
        return positions
    
    def calculate_market_price(self, asset_id: str) -> Optional[float]:
        """Calculate current market price from recent OrderFilled events."""
        positions = self.get_positions_by_asset(asset_id)
        
        if not positions:
            return None
        
        # Use most recent position price
        latest_position = max(positions, key=lambda p: p.timestamp)
        return latest_position.price
    
    def get_trader_summary(self, trader_address: str, positions: Optional[List[Position]] = None) -> Dict:
        """Get summary statistics for a trader.
        
        Args:
            trader_address: The trader address to summarize
            positions: Optional list of positions to use. If not provided, filters from self.positions.
        """
        if positions is None:
            positions = [p for p in self.positions if p.trader_address.lower() == trader_address.lower()]
        
        if not positions:
            return {
                "trader": trader_address,
                "total_positions": 0,
                "total_volume": 0,
                "avg_price": 0,
                "unique_assets": 0
            }
        
        total_volume = sum(p.amount * p.price for p in positions)
        avg_price = sum(p.price for p in positions) / len(positions)
        unique_assets = len(set(p.asset_id for p in positions))
        
        return {
            "trader": trader_address,
            "total_positions": len(positions),
            "total_volume": total_volume,
            "avg_price": avg_price,
            "unique_assets": unique_assets,
            "positions": [p.to_dict() for p in positions]
        }
    
    def get_question_details(self, asset_id: str) -> Optional[str]:
        """Get question details by running 3 queries in sequence:
        1. Query TokenRegistered events by asset ID to get conditionId
        2. Query QuestionInitialized/ConditionPreparation events by conditionId to get questionId
        3. Query question data by questionId to get ancillaryData
        
        Args:
            asset_id: The asset ID to look up
            
        Returns:
            Decoded ancillary_data string, or None if not found
        """
        try:
            # Step 1: Query TokenRegistered events by asset ID to get conditionId
            print(f"Step 1: Querying TokenRegistered events for asset ID: {asset_id}")
            token_events = self.client.get_token_registered_by_asset_id(asset_id, limit=10, since_days=10)
            
            if not token_events:
                print(f"No TokenRegistered events found for asset ID: {asset_id}")
                return None
            
            # Extract conditionId from the first event
            condition_id = None
            for event in token_events:
                args = event.get("Arguments", [])
                arg_dict = {arg["Name"]: arg["Value"] for arg in args}
                
                # Look for conditionId in arguments (can be Bytes or String)
                for key in ["conditionId", "conditionID", "condition_id"]:
                    if key in arg_dict:
                        condition_id_hex = extract_string_value(arg_dict[key])
                        if condition_id_hex:
                            # Remove '0x' prefix if present and normalize
                            condition_id = condition_id_hex.replace("0x", "").lower()
                            break
                
                if condition_id:
                    break
            
            if not condition_id:
                print(f"Could not extract conditionId from TokenRegistered events")
                return None
            
            print(f"Found conditionId: {condition_id}")
            
            # Step 2: Query QuestionInitialized/ConditionPreparation events by conditionId to get questionId
            print(f"Step 2: Querying question events for conditionId: {condition_id}")
            question_events = self.client.get_question_events_by_condition_id(condition_id, limit=1, since_days=10)
            
            if not question_events:
                print(f"No question events found for conditionId: {condition_id}")
                return None
            
            # Extract questionId from the first event
            question_id = None
            for event in question_events:
                args = event.get("Arguments", [])
                arg_dict = {arg["Name"]: arg["Value"] for arg in args}
                
                # Look for questionID or questionId in arguments
                for key in ["questionID", "questionId", "question_id"]:
                    if key in arg_dict:
                        question_id_hex = extract_string_value(arg_dict[key])
                        if question_id_hex:
                            # Remove '0x' prefix if present
                            question_id = question_id_hex.replace("0x", "").lower()
                            break
                
                if question_id:
                    break
            
            if not question_id:
                print(f"Could not extract questionId from question events")
                return None
            
            print(f"Found questionId: {question_id}")
            
            # Step 3: Query QuestionInitialized events by questionId to get ancillaryData
            print(f"Step 3: Querying QuestionInitialized events for questionId: {question_id}")
            question_data_events = self.client.get_question_data_by_question_id(question_id, limit=1, since_days=10)
            
            if not question_data_events:
                print(f"No QuestionInitialized events found for questionId: {question_id}")
                return None
            
            # Extract ancillaryData from the events
            ancillary_data_hex = None
            for event in question_data_events:
                args = event.get("Arguments", [])
                arg_dict = {arg["Name"]: arg["Value"] for arg in args}
                
                # Debug: print all argument names
                print(f"Available argument names: {list(arg_dict.keys())}")
                
                # Look for ancillaryData in arguments (should be named "ancillaryData")
                if "ancillaryData" in arg_dict:
                    print(f"Found ancillaryData argument")
                    ancillary_data_hex = extract_string_value(arg_dict["ancillaryData"])
                    if ancillary_data_hex:
                        print(f"Extracted ancillary_data_hex (length: {len(ancillary_data_hex)})")
                        break
                else:
                    # Try alternative names as fallback
                    for key in ["ancillary_data", "data", "ancillary"]:
                        if key in arg_dict:
                            print(f"Found argument: {key}")
                            ancillary_data_hex = extract_string_value(arg_dict[key])
                            if ancillary_data_hex:
                                print(f"Extracted ancillary_data_hex (length: {len(ancillary_data_hex)})")
                                break
                        if ancillary_data_hex:
                            break
                
                if ancillary_data_hex:
                    break
            
            if not ancillary_data_hex:
                print(f"Could not extract ancillaryData from question events")
                print(f"Tried argument names: ancillaryData, ancillary_data, data, ancillary, questionData, question_data")
                # Debug: show all argument values from the first available event
                debug_event = question_events[0] if question_events else None
                if not debug_event and 'question_data_events' in locals() and question_data_events:
                    debug_event = question_data_events[0]
                
                if debug_event:
                    args = debug_event.get("Arguments", [])
                    print(f"All arguments in event:")
                    for arg in args:
                        arg_name = arg.get("Name", "N/A")
                        arg_value = arg.get("Value", {})
                        value_str = extract_string_value(arg_value) or str(arg_value)
                        print(f"  - {arg_name}: {value_str[:100]}...")
                return None
            
            # Decode hex string to readable string
            try:
                # Remove '0x' prefix if present
                hex_string = ancillary_data_hex.replace("0x", "")
                
                # Convert hex to bytes, then decode to string
                # Handle both even and odd length hex strings
                if len(hex_string) % 2 != 0:
                    hex_string = "0" + hex_string
                
                bytes_data = bytes.fromhex(hex_string)
                ancillary_data = bytes_data.decode('utf-8', errors='ignore').strip('\x00')
                
                print(f"Successfully retrieved and decoded ancillary_data for asset ID: {asset_id}")
                return ancillary_data
            except Exception as decode_error:
                print(f"Error decoding ancillary_data from hex: {decode_error}")
                # Return the hex string if decoding fails
                return ancillary_data_hex
            
        except Exception as e:
            print(f"Error getting question details: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_top_traders_and_assets(
        self,
        limit: int = 20000,
        top_traders_count: int = 20,
        top_assets_count: int = 20
    ) -> Dict:
        """Fetch trades and analyze top traders and asset IDs by volume.
        
        Args:
            limit: Number of trades to fetch (default: 20000)
            top_traders_count: Number of top traders to return (default: 20)
            top_assets_count: Number of top assets to return (default: 20)
            
        Returns:
            Dictionary with:
            - 'traders': List of top traders sorted by total volume
            - 'assets': List of top assets sorted by total volume
            - 'total_trades': Total number of trades analyzed
            - 'total_parsed': Number of trades successfully parsed
        """
        print(f"Fetching {limit} trades...")
        
        # Fetch events
        events = self.client.get_order_filled_events(limit=limit)
        
        if not events:
            return {
                "traders": [],
                "assets": [],
                "total_trades": 0,
                "total_parsed": 0
            }
        
        # Parse all events into positions
        positions = []
        for event in events:
            position = self.parse_order_filled_event(event)
            if position:
                positions.append(position)
        
        print(f"Successfully parsed {len(positions)} positions from {len(events)} events")
        
        # Aggregate by trader
        trader_stats = {}
        for pos in positions:
            trader = pos.trader_address.lower()
            if not trader:
                continue
            
            if trader not in trader_stats:
                trader_stats[trader] = {
                    "address": trader,
                    "total_volume": 0.0,
                    "total_trades": 0,
                    "unique_assets": set(),
                    "avg_price": 0.0,
                    "total_amount": 0.0
                }
            
            volume = pos.amount * pos.price
            trader_stats[trader]["total_volume"] += volume
            trader_stats[trader]["total_trades"] += 1
            trader_stats[trader]["unique_assets"].add(pos.asset_id)
            trader_stats[trader]["total_amount"] += pos.amount
        
        # Convert sets to counts and calculate averages
        for trader in trader_stats:
            stats = trader_stats[trader]
            stats["unique_assets_count"] = len(stats["unique_assets"])
            stats["unique_assets"] = list(stats["unique_assets"])[:10]  # Keep first 10 for display
            if stats["total_trades"] > 0:
                stats["avg_price"] = stats["total_volume"] / stats["total_amount"] if stats["total_amount"] > 0 else 0.0
        
        # Aggregate by asset ID
        asset_stats = {}
        for pos in positions:
            asset_id = str(pos.asset_id).strip()
            if not asset_id:
                continue
            
            if asset_id not in asset_stats:
                asset_stats[asset_id] = {
                    "asset_id": asset_id,
                    "total_volume": 0.0,
                    "total_trades": 0,
                    "unique_traders": set(),
                    "avg_price": 0.0,
                    "total_amount": 0.0
                }
            
            volume = pos.amount * pos.price
            asset_stats[asset_id]["total_volume"] += volume
            asset_stats[asset_id]["total_trades"] += 1
            asset_stats[asset_id]["unique_traders"].add(pos.trader_address.lower())
            asset_stats[asset_id]["total_amount"] += pos.amount
        
        # Convert sets to counts and calculate averages
        for asset_id in asset_stats:
            stats = asset_stats[asset_id]
            stats["unique_traders_count"] = len(stats["unique_traders"])
            if stats["total_trades"] > 0:
                stats["avg_price"] = stats["total_volume"] / stats["total_amount"] if stats["total_amount"] > 0 else 0.0
        
        # Sort traders by total volume (descending)
        top_traders = sorted(
            trader_stats.values(),
            key=lambda x: x["total_volume"],
            reverse=True
        )[:top_traders_count]
        
        # Sort assets by total volume (descending)
        top_assets = sorted(
            asset_stats.values(),
            key=lambda x: x["total_volume"],
            reverse=True
        )[:top_assets_count]
        
        return {
            "traders": top_traders,
            "assets": top_assets,
            "total_trades": len(events),
            "total_parsed": len(positions)
        }

    def get_orderbook(self, asset_id: str, limit: int = 200) -> Dict:
        """Reconstruct recent orderbook snapshot from recent order filled events.
        
        This method reconstructs what the orderbook looked like based on completed trades.
        It processes trades chronologically (oldest first) to build up orderbook depth,
        showing the state as of the most recent trade.
        
        Args:
            asset_id: The asset ID to get orderbook for
            limit: Maximum number of recent events to analyze
            
        Returns:
            Dictionary with 'bids' and 'asks' lists, each containing price levels
            with price, amount, and count. Also includes 'snapshot_time' timestamp.
        """
        events = self.client.get_order_filled_events_by_asset_id(asset_id=asset_id, limit=limit)
        
        if not events:
            return {
                "bids": [],
                "asks": [],
                "asset_id": asset_id,
                "snapshot_time": None
            }
        
        # Parse events into positions
        positions = []
        for event in events:
            position = self.parse_order_filled_event(event)
            if position and str(position.asset_id).strip() == str(asset_id).strip():
                positions.append(position)
        
        if not positions:
            return {
                "bids": [],
                "asks": [],
                "asset_id": asset_id,
                "snapshot_time": None
            }
        
        # Sort positions chronologically (oldest first) to build orderbook over time
        positions.sort(key=lambda p: p.timestamp)
        
        # Track orderbook depth at each price level
        # Bids: buy orders (what buyers were willing to pay)
        # Asks: sell orders (what sellers were willing to accept)
        bid_levels = {}  # price -> {amount, count, last_trade_time}
        ask_levels = {}  # price -> {amount, count, last_trade_time}
        
        # Process each trade chronologically to build up orderbook depth
        for position in positions:
            price = position.price
            amount = position.amount
            
            # Determine buyer and seller
            buyer_address = position.trader_address  # The one receiving tokens (buying)
            
            # Determine seller: the party that is NOT the buyer
            seller_address = None
            if position.maker_address and position.taker_address:
                if buyer_address:
                    buyer_lower = buyer_address.lower()
                    maker_lower = position.maker_address.lower()
                    taker_lower = position.taker_address.lower()
                    
                    if buyer_lower == maker_lower:
                        seller_address = position.taker_address
                    elif buyer_lower == taker_lower:
                        seller_address = position.maker_address
                    else:
                        # Fallback: use the non-buyer address
                        seller_address = position.maker_address if maker_lower != buyer_lower else position.taker_address
                else:
                    # If no buyer identified, use maker as seller (fallback)
                    seller_address = position.maker_address
            
            # Add to bids: buyer's willingness to pay at this price
            if buyer_address:
                if price not in bid_levels:
                    bid_levels[price] = {"amount": 0.0, "count": 0, "last_trade_time": position.timestamp}
                bid_levels[price]["amount"] += amount
                bid_levels[price]["count"] += 1
                # Update to most recent trade time at this price level
                if position.timestamp > bid_levels[price]["last_trade_time"]:
                    bid_levels[price]["last_trade_time"] = position.timestamp
            
            # Add to asks: seller's willingness to accept at this price
            if seller_address:
                if price not in ask_levels:
                    ask_levels[price] = {"amount": 0.0, "count": 0, "last_trade_time": position.timestamp}
                ask_levels[price]["amount"] += amount
                ask_levels[price]["count"] += 1
                # Update to most recent trade time at this price level
                if position.timestamp > ask_levels[price]["last_trade_time"]:
                    ask_levels[price]["last_trade_time"] = position.timestamp
        
        # Convert to sorted lists for display
        # Bids: sorted by price descending (highest bid first) - best bid at top
        bids = [
            {
                "price": price,
                "amount": level["amount"],
                "count": level["count"]
            }
            for price, level in sorted(bid_levels.items(), reverse=True)
        ]
        
        # Asks: sorted by price ascending (lowest ask first) - best ask at top
        asks = [
            {
                "price": price,
                "amount": level["amount"],
                "count": level["count"]
            }
            for price, level in sorted(ask_levels.items())
        ]
        
        # Get snapshot time (most recent trade timestamp)
        snapshot_time = max(p.timestamp for p in positions) if positions else None
        
        return {
            "bids": bids,
            "asks": asks,
            "asset_id": asset_id,
            "total_events": len(events),
            "total_positions": len(positions),
            "snapshot_time": snapshot_time
        }

