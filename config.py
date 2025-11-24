"""Configuration management for Polymarket Copy Trading Tool."""
import os
from dotenv import load_dotenv

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

class Config:
    """Application configuration."""
    
    # Bitquery API
    OAUTH_TOKEN = os.getenv("OAUTH_TOKEN", "")
    BITQUERY_API_URL = os.getenv("BITQUERY_API_URL", "https://streaming.bitquery.io/graphql")
    
    # Wallet auth
    SEED_PHRASE = os.getenv("SEED_PHRASE", "").strip()
    
    # Polymarket API Configuration
    POLYMARKET_HOST = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    POLYMARKET_CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))  # Polygon mainnet
    POLYMARKET_SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "1"))  # 1=Email/Magic, 2=Browser Wallet, None=EOA
    POLYMARKET_PROXY_ADDRESS = os.getenv("POLYMARKET_PROXY_ADDRESS", "")  # Proxy address for funding
    POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
    POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
    POLYMARKET_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
    
    # Contract Addresses
    CTF_EXCHANGE_ADDRESS = os.getenv(
        "CTF_EXCHANGE_ADDRESS", 
        "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    )
    LEGACY_EXCHANGE_ADDRESS = os.getenv(
        "LEGACY_EXCHANGE_ADDRESS",
        "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
    )
    # USDC contract address on Polygon
    # Native USDC: 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359
    # Bridged USDC.e: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
    USDC_ADDRESS = os.getenv(
        "USDC_ADDRESS",
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # Native USDC (default)
    )
    
    # Network
    NETWORK = "matic"
    DATASET = "realtime"  # or "combined" for historical
    
    # Copy Trading Defaults
    DEFAULT_COPY_AMOUNT_USD = float(os.getenv("DEFAULT_COPY_AMOUNT_USD", "0.001"))  # Default: 0.001 USD per trade
    
    @classmethod
    def validate(cls):
        """Validate required configuration."""
        if not cls.OAUTH_TOKEN:
            raise ValueError("OAUTH_TOKEN is required in .env file")
        return True

