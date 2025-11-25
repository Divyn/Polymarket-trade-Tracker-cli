"""Utilities for decoding and categorizing Polymarket question data."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from collections import Counter
import re


@dataclass
class QuestionAnalysis:
    """Structured representation of a decoded question event."""

    question_id: Optional[str]
    condition_id: Optional[str]
    ancillary_text: str
    block_time: Optional[datetime]
    block_number: Optional[int]
    tx_hash: Optional[str]
    topics: List[str]
    keywords: List[str]


class QuestionAnalyzer:
    """Decode ancillaryData payloads and summarize their content."""

    _STOPWORDS = {
        "the",
        "and",
        "for",
        "that",
        "with",
        "will",
        "this",
        "from",
        "have",
        "when",
        "what",
        "which",
        "would",
        "about",
        "there",
        "their",
        "been",
        "into",
        "more",
        "than",
        "over",
        "shall",
        "should",
        "could",
        "while",
        "against",
        "among",
        "upon",
        "where",
        "after",
        "before",
        "within",
        "between",
        "such",
        "also",
        "does",
        "ever",
        "whose",
        "each",
        "here",
        "into",
        "your",
        "you",
        "are",
        "was",
        "did",
        "has",
        "its",
        "our",
        "who",
        "how",
        "why",
        "can",
        "may",
        "per",
        "any",
        "market",
        "markets",
        "resolve",
        "resolved",
        "question",
        "upcoming",
        "res",
        "data",
        "res_data",
        "initializer",
        "https",
        "http",
        "market_id",
    }

    _TOPIC_KEYWORDS: Dict[str, List[str]] = {
        "Politics": [
            "election",
            "president",
            "prime",
            "minister",
            "parliament",
            "vote",
            "senate",
            "congress",
            "policy",
            "government",
        ],
        "Crypto": [
            "bitcoin",
            "btc",
            "ethereum",
            "eth",
            "polygon",
            "matic",
            "sol",
            "solana",
            "xrp",
            "usdt",
            "token",
            "crypto",
            "defi",
            "blockchain",
            "stablecoin",
            "sec",
            "binance",
        ],
        "Finance": [
            "stock",
            "stocks",
            "price",
            "interest",
            "rate",
            "rates",
            "inflation",
            "gdp",
            "revenue",
            "profit",
            "yield",
            "bond",
            "bonds",
            "treasury",
            "fed",
        ],
        "Sports": [
            "game",
            "match",
            "league",
            "season",
            "championship",
            "tournament",
            "score",
            "team",
            "player",
            "coach",
            "nfl",
            "nba",
            "mlb",
            "nhl",
            "cbb",
            "soccer",
            "football",
            "basketball",
            "ufc",
            "mls",
        ],
        "Geopolitics": [
            "war",
            "conflict",
            "russia",
            "ukraine",
            "china",
            "sanction",
            "military",
            "peace",
            "nato",
            "border",
        ],
        "Technology": [
            "artificial",
            "intelligence",
            "software",
            "hardware",
            "chip",
            "semiconductor",
            "robotics",
            "cyber",
            "cloud",
            "compute",
            "machine",
            "learning",
        ],
        "Weather": [
            "temperature",
            "rain",
            "snow",
            "storm",
            "hurricane",
            "weather",
            "climate",
            "heat",
            "cold",
            "precipitation",
        ],
        "Health": [
            "covid",
            "virus",
            "vaccine",
            "disease",
            "hospital",
            "cases",
            "death",
            "health",
            "cdc",
            "who",
        ],
    }

    _WORD_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9']+")

    _FIELD_PATTERNS = {
        "title": re.compile(
            r"title:\s*(?P<value>.*?)(?:,\s*description:|$)",
            re.IGNORECASE | re.DOTALL,
        ),
        "description": re.compile(
            r"description:\s*(?P<value>.*?)(?:market_id:|res_data:|initializer:|updates made|https?://|$)",
            re.IGNORECASE | re.DOTALL,
        ),
    }

    def __init__(self, max_keywords: int = 8):
        self.max_keywords = max_keywords

    def analyze_events(self, events: List[Dict[str, Any]]) -> List[QuestionAnalysis]:
        """Convert raw Bitquery events into structured analyses."""
        analyses: List[QuestionAnalysis] = []
        for event in events:
            analysis = self._analyze_single_event(event)
            if analysis:
                analyses.append(analysis)
        return analyses

    def _analyze_single_event(self, event: Dict[str, Any]) -> Optional[QuestionAnalysis]:
        arguments = self._normalize_arguments(event.get("Arguments", []))
        ancillary_hex = arguments.get("ancillaryData")
        if not ancillary_hex:
            return None

        ancillary_text = self._decode_ancillary_data(ancillary_hex)
        topics = self._detect_topics(ancillary_text)
        keywords = self._extract_keywords(ancillary_text)

        block_info = event.get("Block") or {}
        block_time = self._parse_time(block_info.get("Time"))

        return QuestionAnalysis(
            question_id=arguments.get("questionID"),
            condition_id=arguments.get("conditionId"),
            ancillary_text=ancillary_text.strip(),
            block_time=block_time,
            block_number=block_info.get("Number"),
            tx_hash=(event.get("Transaction") or {}).get("Hash"),
            topics=topics,
            keywords=keywords,
        )

    @staticmethod
    def _normalize_arguments(arguments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Flatten Bitquery argument array into a name/value mapping."""
        normalized: Dict[str, Any] = {}
        for argument in arguments or []:
            name = argument.get("Name")
            value_obj = argument.get("Value") or {}
            if not name:
                continue
            normalized[name] = (
                value_obj.get("string")
                or value_obj.get("hex")
                or value_obj.get("bigInteger")
                or value_obj.get("integer")
                or value_obj.get("address")
                or value_obj.get("bool")
            )
        return normalized

    @staticmethod
    def _decode_ancillary_data(hex_value: str) -> str:
        """Decode ancillaryData hex payloads into UTF-8 text."""
        if not hex_value:
            return ""
        value = hex_value[2:] if hex_value.startswith("0x") else hex_value
        try:
            return bytes.fromhex(value).decode("utf-8", errors="replace")
        except ValueError:
            # Already plain-text or non-hex data
            return hex_value

    @staticmethod
    def _parse_time(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _extract_keywords(self, text: str) -> List[str]:
        title_text = self._extract_field_value(text, "title")
        description_text = self._extract_field_value(text, "description")
        counts: Counter[str] = Counter()

        for token in self._tokenize(title_text):
            if token not in self._STOPWORDS and len(token) > 2:
                counts[token] += 3  # elevate teams, assets, or events in the title

        for token in self._tokenize(description_text):
            if token not in self._STOPWORDS and len(token) > 2:
                counts[token] += 1

        if not counts:
            for token in self._tokenize(text):
                if token not in self._STOPWORDS and len(token) > 2:
                    counts[token] += 1

        if not counts:
            return []

        return [word for word, _ in counts.most_common(self.max_keywords)]

    def _detect_topics(self, text: str) -> List[str]:
        core_text = self._extract_core_text(text)
        tokens = set(self._tokenize(core_text))

        scored_topics: List[str] = []
        for topic, keywords in self._TOPIC_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in tokens)
            if score:
                scored_topics.append((topic, score))

        if not scored_topics:
            return ["General"]

        scored_topics.sort(key=lambda item: item[1], reverse=True)
        return [topic for topic, _ in scored_topics[:3]]

    def _extract_field_value(self, text: str, field: str) -> str:
        pattern = self._FIELD_PATTERNS.get(field)
        if not pattern:
            return ""
        match = pattern.search(text)
        if not match:
            return ""
        return " ".join(match.group("value").split())

    def _extract_core_text(self, text: str) -> str:
        parts = [
            part
            for part in (
                self._extract_field_value(text, "title"),
                self._extract_field_value(text, "description"),
            )
            if part
        ]
        if parts:
            return " ".join(parts)
        return " ".join(text.split())

    def _tokenize(self, text: str) -> List[str]:
        if not text:
            return []
        return self._WORD_PATTERN.findall(text.lower())


