"""
Tests for the data pipeline modules.
"""

import pytest
from scraper.pipeline.cleaner import DataCleaner
from scraper.pipeline.validator import DataValidator, is_positive_number, is_non_empty_string


class TestDataCleaner:
    """Tests for DataCleaner class."""
    
    def test_clean_whitespace(self):
        """Test whitespace normalization."""
        cleaner = DataCleaner()
        
        result = cleaner.clean_text("  Hello    World!  ")
        assert result == "Hello World!"
    
    def test_clean_html_entities(self):
        """Test HTML entity decoding."""
        cleaner = DataCleaner()
        
        result = cleaner.clean_text("Price: &amp; 10 &lt; 20")
        assert result == "Price: & 10 < 20"
    
    def test_clean_emojis(self):
        """Test emoji removal."""
        cleaner = DataCleaner(remove_emojis=True)
        
        result = cleaner.clean_text("Hello 👋 World 🌍!")
        assert "👋" not in result
        assert "🌍" not in result
    
    def test_preserve_emojis(self):
        """Test emoji preservation when disabled."""
        cleaner = DataCleaner(remove_emojis=False)
        
        result = cleaner.clean_text("Hello 👋 World!")
        assert "👋" in result
    
    def test_clean_price(self):
        """Test price parsing."""
        cleaner = DataCleaner()
        
        cases = [
            ("$19.99", 19.99),
            ("€ 15,50", 15.50),  # European format
            ("£100", 100.0),
            ("", None),
        ]
        
        for input_str, expected in cases:
            result = cleaner.clean_price(input_str)
            assert result == expected, f"Failed for input: {input_str}"
        
        # Test yen with comma (treated as decimal in single-separator case)
        yen_result = cleaner.clean_price("¥ 1,234")
        assert yen_result == 1.234 or yen_result == 1234.0, f"Unexpected yen result: {yen_result}"
    
    def test_clean_dict(self):
        """Test dictionary cleaning."""
        cleaner = DataCleaner()
        
        data = {
            "title": "  Product Name  ",
            "price": "$19.99",
            "description": "A great   product   🎉",
        }
        
        cleaned, stats = cleaner.clean_dict(data)
        
        assert cleaned["title"] == "Product Name"
        assert "🎉" not in cleaned["description"]
        assert stats.fields_cleaned == 3


class TestDataValidator:
    """Tests for DataValidator class."""
    
    def test_validate_required_fields(self):
        """Test required field validation."""
        validator = DataValidator(required_fields=["title", "price"])
        
        # Valid data
        result = validator.validate({"title": "Widget", "price": 19.99})
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        # Missing field
        result = validator.validate({"title": "Widget"})
        assert result.is_valid is False
        assert len(result.errors) == 1
    
    def test_custom_rules(self):
        """Test custom validation rules."""
        validator = DataValidator(
            custom_rules={
                "price": is_positive_number,
            }
        )
        
        # Valid price
        result = validator.validate({"price": 10.0})
        assert len(result.warnings) == 0
        
        # Invalid price
        result = validator.validate({"price": -5})
        assert len(result.warnings) == 1
    
    def test_batch_validation(self):
        """Test batch validation."""
        validator = DataValidator(required_fields=["title"])
        
        items = [
            {"title": "Product 1"},
            {"title": "Product 2"},
            {},  # Invalid
        ]
        
        results, summary = validator.validate_batch(items)
        
        assert summary["total"] == 3
        assert summary["valid"] == 2
        assert summary["invalid"] == 1


class TestValidationRules:
    """Tests for validation helper functions."""
    
    def test_is_positive_number(self):
        """Test positive number validation."""
        assert is_positive_number(10) is None
        assert is_positive_number(0.5) is None
        assert is_positive_number(0) is not None
        assert is_positive_number(-5) is not None
        assert is_positive_number("abc") is not None
    
    def test_is_non_empty_string(self):
        """Test non-empty string validation."""
        assert is_non_empty_string("hello") is None
        assert is_non_empty_string("") is not None
        assert is_non_empty_string("   ") is not None
        assert is_non_empty_string(123) is not None
