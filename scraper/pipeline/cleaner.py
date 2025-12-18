"""
Data Cleaner Module

Cleans and normalizes scraped data.
Removes emojis, currency symbols, extra whitespace, etc.
"""

import re
import html
import unicodedata
from dataclasses import dataclass
from typing import Any, Callable, Dict, List


@dataclass
class CleaningStats:
    """Statistics from cleaning operation."""
    
    fields_cleaned: int
    chars_removed: int
    original_size: int
    cleaned_size: int
    
    @property
    def size_reduction(self) -> float:
        """Calculate size reduction percentage."""
        if self.original_size == 0:
            return 0.0
        return ((self.original_size - self.cleaned_size) / self.original_size) * 100


class DataCleaner:
    """
    Cleans and normalizes scraped text data.
    
    Features:
    - Whitespace normalization
    - HTML entity decoding
    - Emoji removal
    - Currency symbol handling
    - Unicode normalization
    - Custom cleaning rules
    
    Example:
        cleaner = DataCleaner()
        cleaned = cleaner.clean_text("  Hello   World!  ")
        # Returns: "Hello World!"
    """
    
    # Emoji pattern (covers most common emojis)
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & pictographs
        "\U0001F680-\U0001F6FF"  # Transport & map
        "\U0001F1E0-\U0001F1FF"  # Flags
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"  # Enclosed characters
        "]+",
        flags=re.UNICODE,
    )
    
    # Currency symbols
    CURRENCY_SYMBOLS = re.compile(r'[$€£¥₹₽₿¢₩₪]')
    
    # Multiple whitespace
    MULTI_WHITESPACE = re.compile(r'\s+')
    
    # Control characters
    CONTROL_CHARS = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')
    
    def __init__(
        self,
        remove_emojis: bool = True,
        remove_currency: bool = False,
        normalize_unicode: bool = True,
        decode_html: bool = True,
        strip_whitespace: bool = True,
        lowercase: bool = False,
    ):
        """
        Initialize the cleaner.
        
        Args:
            remove_emojis: Remove emoji characters
            remove_currency: Remove currency symbols
            normalize_unicode: Normalize Unicode (NFKC)
            decode_html: Decode HTML entities
            strip_whitespace: Normalize whitespace
            lowercase: Convert to lowercase
        """
        self._remove_emojis = remove_emojis
        self._remove_currency = remove_currency
        self._normalize_unicode = normalize_unicode
        self._decode_html = decode_html
        self._strip_whitespace = strip_whitespace
        self._lowercase = lowercase
        
        self._custom_cleaners: List[Callable[[str], str]] = []
    
    def add_cleaner(self, func: Callable[[str], str]) -> None:
        """Add a custom cleaning function."""
        self._custom_cleaners.append(func)
    
    def clean_text(self, text: str) -> str:
        """
        Clean a single text string.
        
        Args:
            text: Text to clean
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        result = text
        
        # Decode HTML entities
        if self._decode_html:
            result = html.unescape(result)
        
        # Remove control characters
        result = self.CONTROL_CHARS.sub('', result)
        
        # Normalize Unicode
        if self._normalize_unicode:
            result = unicodedata.normalize("NFKC", result)
        
        # Remove emojis
        if self._remove_emojis:
            result = self.EMOJI_PATTERN.sub('', result)
        
        # Handle currency symbols
        if self._remove_currency:
            result = self.CURRENCY_SYMBOLS.sub('', result)
        
        # Normalize whitespace
        if self._strip_whitespace:
            result = self.MULTI_WHITESPACE.sub(' ', result)
            result = result.strip()
        
        # Lowercase
        if self._lowercase:
            result = result.lower()
        
        # Apply custom cleaners
        for cleaner in self._custom_cleaners:
            result = cleaner(result)
        
        return result
    
    def clean_price(self, price_str: str) -> float | None:
        """
        Clean and parse a price string.
        
        Args:
            price_str: Price string (e.g., "$19.99", "€ 15,50")
            
        Returns:
            Float price or None if invalid
        """
        if not price_str:
            return None
        
        # Remove currency symbols and whitespace
        cleaned = self.CURRENCY_SYMBOLS.sub('', price_str)
        cleaned = cleaned.strip()
        
        # Handle European format (comma as decimal)
        if ',' in cleaned and '.' in cleaned:
            # Assume last separator is decimal
            cleaned = cleaned.replace('.', '').replace(',', '.')
        elif ',' in cleaned:
            cleaned = cleaned.replace(',', '.')
        
        # Extract numeric part
        match = re.search(r'[\d.]+', cleaned)
        if match:
            try:
                return float(match.group())
            except ValueError:
                pass
        
        return None
    
    def clean_dict(
        self,
        data: Dict[str, Any],
        fields: List[str] | None = None,
    ) -> tuple[Dict[str, Any], CleaningStats]:
        """
        Clean all string values in a dictionary.
        
        Args:
            data: Dictionary with values to clean
            fields: Specific fields to clean (all if None)
            
        Returns:
            Tuple of (cleaned dict, stats)
        """
        cleaned = data.copy()
        original_size = 0
        cleaned_size = 0
        fields_cleaned = 0
        
        for key, value in data.items():
            if fields and key not in fields:
                continue
            
            if isinstance(value, str):
                original_size += len(value)
                cleaned_value = self.clean_text(value)
                cleaned[key] = cleaned_value
                cleaned_size += len(cleaned_value)
                fields_cleaned += 1
            elif isinstance(value, dict):
                # Recursively clean nested dicts
                nested_cleaned, nested_stats = self.clean_dict(value, fields)
                cleaned[key] = nested_cleaned
                original_size += nested_stats.original_size
                cleaned_size += nested_stats.cleaned_size
                fields_cleaned += nested_stats.fields_cleaned
            elif isinstance(value, list):
                # Clean string items in lists
                cleaned_list = []
                for item in value:
                    if isinstance(item, str):
                        original_size += len(item)
                        cleaned_item = self.clean_text(item)
                        cleaned_list.append(cleaned_item)
                        cleaned_size += len(cleaned_item)
                        fields_cleaned += 1
                    else:
                        cleaned_list.append(item)
                cleaned[key] = cleaned_list
        
        stats = CleaningStats(
            fields_cleaned=fields_cleaned,
            chars_removed=original_size - cleaned_size,
            original_size=original_size,
            cleaned_size=cleaned_size,
        )
        
        return cleaned, stats
    
    def extract_text(self, html_content: str) -> str:
        """
        Extract and clean visible text from HTML.
        
        Args:
            html_content: HTML string
            
        Returns:
            Cleaned visible text
        """
        # Remove script and style tags
        cleaned = re.sub(
            r'<script[^>]*>[\s\S]*?</script>',
            '',
            html_content,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r'<style[^>]*>[\s\S]*?</style>',
            '',
            cleaned,
            flags=re.IGNORECASE,
        )
        
        # Remove HTML comments
        cleaned = re.sub(r'<!--[\s\S]*?-->', '', cleaned)
        
        # Remove HTML tags
        cleaned = re.sub(r'<[^>]+>', ' ', cleaned)
        
        # Clean the text
        return self.clean_text(cleaned)


# Preset cleaners for common use cases
def create_strict_cleaner() -> DataCleaner:
    """Create a strict cleaner that removes most special chars."""
    cleaner = DataCleaner(
        remove_emojis=True,
        remove_currency=True,
        normalize_unicode=True,
        decode_html=True,
        strip_whitespace=True,
    )
    
    # Add custom cleaner to remove special chars
    cleaner.add_cleaner(
        lambda s: re.sub(r'[^\w\s.,!?-]', '', s)
    )
    
    return cleaner


def create_minimal_cleaner() -> DataCleaner:
    """Create a minimal cleaner that preserves most content."""
    return DataCleaner(
        remove_emojis=False,
        remove_currency=False,
        normalize_unicode=True,
        decode_html=True,
        strip_whitespace=True,
    )
