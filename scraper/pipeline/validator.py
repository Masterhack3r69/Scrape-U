"""
Data Validator Module

Validates scraped data against expected schemas.
Flags invalid or incomplete records for review.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, ValidationError


class ValidationSeverity(Enum):
    """Severity level of validation issues."""
    ERROR = "error"      # Critical - data unusable
    WARNING = "warning"  # Non-critical - data may be incomplete
    INFO = "info"        # Informational only


@dataclass
class ValidationIssue:
    """A single validation issue."""
    
    field: str
    message: str
    severity: ValidationSeverity
    value: Any = None


@dataclass
class ValidationResult:
    """Result of data validation."""
    
    is_valid: bool
    data: Dict[str, Any]
    issues: List[ValidationIssue] = field(default_factory=list)
    cleaned_data: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def errors(self) -> List[ValidationIssue]:
        """Get only error-level issues."""
        return [i for i in self.issues if i.severity == ValidationSeverity.ERROR]
    
    @property
    def warnings(self) -> List[ValidationIssue]:
        """Get only warning-level issues."""
        return [i for i in self.issues if i.severity == ValidationSeverity.WARNING]
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "data": self.data,
            "cleaned_data": self.cleaned_data,
            "issues": [
                {
                    "field": i.field,
                    "message": i.message,
                    "severity": i.severity.value,
                }
                for i in self.issues
            ],
        }


class DataValidator:
    """
    Validates scraped data against schemas and custom rules.
    
    Features:
    - Pydantic model validation
    - Custom validation rules
    - Field-level validation
    - Severity-based issue categorization
    
    Example:
        class ProductSchema(BaseModel):
            title: str
            price: float
            
        validator = DataValidator(ProductSchema)
        result = validator.validate({"title": "Widget", "price": "19.99"})
    """
    
    def __init__(
        self,
        schema: type[BaseModel] | None = None,
        required_fields: List[str] | None = None,
        custom_rules: Dict[str, Callable[[Any], Optional[str]]] | None = None,
    ):
        """
        Initialize the validator.
        
        Args:
            schema: Pydantic model for validation (optional)
            required_fields: List of required field names
            custom_rules: Dict of field -> validation function
                         Function returns error message or None if valid
        """
        self._schema = schema
        self._required_fields = required_fields or []
        self._custom_rules = custom_rules or {}
    
    def _validate_required(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """Check required fields are present and non-empty."""
        issues = []
        
        for field in self._required_fields:
            value = data.get(field)
            
            if value is None:
                issues.append(ValidationIssue(
                    field=field,
                    message=f"Required field '{field}' is missing",
                    severity=ValidationSeverity.ERROR,
                ))
            elif isinstance(value, str) and not value.strip():
                issues.append(ValidationIssue(
                    field=field,
                    message=f"Required field '{field}' is empty",
                    severity=ValidationSeverity.ERROR,
                ))
        
        return issues
    
    def _validate_schema(self, data: Dict[str, Any]) -> tuple[Dict[str, Any], List[ValidationIssue]]:
        """Validate against Pydantic schema."""
        issues = []
        cleaned = data.copy()
        
        if not self._schema:
            return cleaned, issues
        
        try:
            validated = self._schema.model_validate(data)
            cleaned = validated.model_dump()
        except ValidationError as e:
            for error in e.errors():
                field = ".".join(str(loc) for loc in error["loc"])
                issues.append(ValidationIssue(
                    field=field,
                    message=error["msg"],
                    severity=ValidationSeverity.ERROR,
                    value=error.get("input"),
                ))
        
        return cleaned, issues
    
    def _validate_custom(self, data: Dict[str, Any]) -> List[ValidationIssue]:
        """Apply custom validation rules."""
        issues = []
        
        for field, rule in self._custom_rules.items():
            value = data.get(field)
            error = rule(value)
            
            if error:
                issues.append(ValidationIssue(
                    field=field,
                    message=error,
                    severity=ValidationSeverity.WARNING,
                    value=value,
                ))
        
        return issues
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate data against all rules.
        
        Args:
            data: Dictionary of scraped data
            
        Returns:
            ValidationResult with status and issues
        """
        all_issues: List[ValidationIssue] = []
        
        # Check required fields
        all_issues.extend(self._validate_required(data))
        
        # Validate against schema
        cleaned, schema_issues = self._validate_schema(data)
        all_issues.extend(schema_issues)
        
        # Apply custom rules
        all_issues.extend(self._validate_custom(data))
        
        # Determine overall validity (no errors)
        is_valid = all(
            issue.severity != ValidationSeverity.ERROR
            for issue in all_issues
        )
        
        return ValidationResult(
            is_valid=is_valid,
            data=data,
            issues=all_issues,
            cleaned_data=cleaned if is_valid else {},
        )
    
    def validate_batch(
        self,
        items: List[Dict[str, Any]],
    ) -> tuple[List[ValidationResult], dict]:
        """
        Validate a batch of items.
        
        Args:
            items: List of data dictionaries
            
        Returns:
            Tuple of (results, summary)
        """
        results = [self.validate(item) for item in items]
        
        valid_count = sum(1 for r in results if r.is_valid)
        
        summary = {
            "total": len(items),
            "valid": valid_count,
            "invalid": len(items) - valid_count,
            "success_rate": (valid_count / len(items) * 100) if items else 0,
        }
        
        return results, summary


# Common validation rules
def is_positive_number(value: Any) -> Optional[str]:
    """Validate that value is a positive number."""
    if value is None:
        return None
    try:
        num = float(value)
        if num <= 0:
            return "Value must be positive"
    except (TypeError, ValueError):
        return "Value must be a number"
    return None


def is_non_empty_string(value: Any) -> Optional[str]:
    """Validate that value is a non-empty string."""
    if not isinstance(value, str):
        return "Value must be a string"
    if not value.strip():
        return "Value cannot be empty"
    return None


def is_valid_url(value: Any) -> Optional[str]:
    """Validate that value is a valid URL."""
    if not isinstance(value, str):
        return "URL must be a string"
    if not value.startswith(("http://", "https://")):
        return "URL must start with http:// or https://"
    return None
