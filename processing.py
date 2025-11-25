"""Utility helpers for normalizing Bitquery argument values."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Tuple

USDC_DECIMALS = Decimal("1e6")
TOKEN_DECIMALS = Decimal("1e6")


def _resolve_value(value: Any) -> Any:
    """Peel nested Bitquery value containers until a primitive is found."""
    if isinstance(value, dict):
        for key in ("bigInteger", "integer", "string", "address", "hex", "bool"):
            if key in value and value[key] is not None:
                return _resolve_value(value[key])
    return value


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned.startswith("0x"):
            try:
                return Decimal(int(cleaned, 16))
            except ValueError:
                return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None
    return None


def extract_value(value_dict: Any) -> Optional[Decimal]:
    """Extract numeric values from Bitquery argument formats."""
    resolved = _resolve_value(value_dict)
    return _to_decimal(resolved)


def extract_string_value(value_dict: Any) -> Optional[str]:
    """Extract string-friendly representation (addresses, IDs, etc.)."""
    resolved = _resolve_value(value_dict)
    if resolved is None:
        return None
    if isinstance(resolved, bytes):
        return resolved.hex()
    return str(resolved)


def process_trade_amounts(usdc_paid: Any, tokens_amount: Any) -> Tuple[float, float, float]:
    """Normalize USDC/token amounts and derive price."""
    usdc_value = _to_decimal(_resolve_value(usdc_paid)) or Decimal(0)
    token_value = _to_decimal(_resolve_value(tokens_amount)) or Decimal(0)

    usdc_normalized = float(usdc_value / USDC_DECIMALS) if usdc_value else 0.0
    tokens_normalized = float(token_value / TOKEN_DECIMALS) if token_value else 0.0

    price = (usdc_normalized / tokens_normalized) if tokens_normalized else 0.0
    return usdc_normalized, tokens_normalized, price

