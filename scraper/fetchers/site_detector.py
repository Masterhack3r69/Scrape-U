"""
Site Detector Module

Analyzes websites to determine if they require browser rendering.
Detects JavaScript frameworks and dynamic content patterns.
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from scraper.fetchers.http_fetcher import FetchResult


class SiteType(Enum):
    """Type of website rendering."""
    STATIC = "static"
    DYNAMIC = "dynamic"
    UNKNOWN = "unknown"


@dataclass
class SiteAnalysis:
    """Result of site analysis."""
    
    url: str
    site_type: SiteType
    confidence: float  # 0.0 to 1.0
    detected_frameworks: list[str]
    requires_browser: bool
    reasons: list[str]


class SiteDetector:
    """
    Detects if a site requires browser rendering.
    
    Analyzes HTTP response for indicators of:
    - JavaScript frameworks (React, Vue, Angular, Next.js)
    - Dynamic loading patterns
    - Empty body with JS scripts
    
    Example:
        detector = SiteDetector()
        analysis = detector.analyze(fetch_result)
        if analysis.requires_browser:
            # Use BrowserFetcher instead
            ...
    """
    
    # Framework detection patterns
    FRAMEWORK_PATTERNS = {
        "react": [
            r'react\.production\.min\.js',
            r'react-dom',
            r'__REACT_DEVTOOLS_GLOBAL_HOOK__',
            r'data-reactroot',
            r'_reactRootContainer',
        ],
        "vue": [
            r'vue\.js',
            r'vue\.min\.js',
            r'data-v-[a-f0-9]+',
            r'__VUE__',
            r'Vue\.js',
        ],
        "angular": [
            r'angular\.js',
            r'angular\.min\.js',
            r'ng-app',
            r'ng-controller',
            r'angular\.module',
        ],
        "next.js": [
            r'_next/static',
            r'__NEXT_DATA__',
            r'next\.js',
        ],
        "nuxt": [
            r'_nuxt',
            r'__NUXT__',
            r'nuxt\.js',
        ],
        "svelte": [
            r'svelte-[a-z0-9]+',
            r'__svelte__',
        ],
    }
    
    # Patterns indicating dynamic content
    DYNAMIC_PATTERNS = [
        r'<div\s+id=["\']app["\']>\s*</div>',  # Empty app container
        r'<div\s+id=["\']root["\']>\s*</div>',  # Empty root container
        r'window\.__INITIAL_STATE__',
        r'window\.__PRELOADED_STATE__',
        r'hydrate\s*\(',
        r'renderToString',
    ]
    
    # Patterns indicating static content
    STATIC_PATTERNS = [
        r'<!DOCTYPE\s+html[^>]*>\s*<html[^>]*>\s*<head',
        r'<article[^>]*>[\s\S]{500,}</article>',  # Substantial article content
        r'<main[^>]*>[\s\S]{500,}</main>',
    ]
    
    def __init__(self, min_content_length: int = 500):
        """
        Initialize the detector.
        
        Args:
            min_content_length: Minimum content length to consider page "loaded"
        """
        self._min_content_length = min_content_length
    
    def _detect_frameworks(self, content: str) -> list[str]:
        """Detect JavaScript frameworks in the content."""
        detected = []
        
        for framework, patterns in self.FRAMEWORK_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, content, re.IGNORECASE):
                    detected.append(framework)
                    break
        
        return detected
    
    def _check_dynamic_patterns(self, content: str) -> list[str]:
        """Check for dynamic content patterns."""
        matches = []
        
        for pattern in self.DYNAMIC_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                matches.append(pattern[:50])
        
        return matches
    
    def _check_static_patterns(self, content: str) -> bool:
        """Check if content appears to be fully static."""
        for pattern in self.STATIC_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False
    
    def _extract_text_content(self, html: str) -> str:
        """Extract visible text from HTML."""
        # Remove script and style tags
        clean = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', html, flags=re.IGNORECASE)
        clean = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', clean, flags=re.IGNORECASE)
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', ' ', clean)
        # Normalize whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean
    
    def analyze(self, result: FetchResult) -> SiteAnalysis:
        """
        Analyze a fetch result to determine site type.
        
        Args:
            result: The FetchResult to analyze
            
        Returns:
            SiteAnalysis with detection results
        """
        content = result.content
        reasons = []
        confidence = 0.5
        
        # Detect frameworks
        frameworks = self._detect_frameworks(content)
        if frameworks:
            reasons.append(f"Detected frameworks: {', '.join(frameworks)}")
            confidence += 0.2
        
        # Check dynamic patterns
        dynamic_patterns = self._check_dynamic_patterns(content)
        if dynamic_patterns:
            reasons.append(f"Found {len(dynamic_patterns)} dynamic pattern(s)")
            confidence += 0.15
        
        # Check text content length
        text_content = self._extract_text_content(content)
        if len(text_content) < self._min_content_length:
            reasons.append(f"Low text content ({len(text_content)} chars)")
            confidence += 0.15
        else:
            confidence -= 0.2
            reasons.append(f"Sufficient text content ({len(text_content)} chars)")
        
        # Check for static patterns
        if self._check_static_patterns(content):
            reasons.append("Content appears fully rendered")
            confidence -= 0.3
        
        # Normalize confidence
        confidence = max(0.0, min(1.0, confidence))
        
        # Determine site type
        if confidence > 0.6:
            site_type = SiteType.DYNAMIC
            requires_browser = True
        elif confidence < 0.4:
            site_type = SiteType.STATIC
            requires_browser = False
        else:
            site_type = SiteType.UNKNOWN
            requires_browser = len(frameworks) > 0
        
        return SiteAnalysis(
            url=result.url,
            site_type=site_type,
            confidence=confidence,
            detected_frameworks=frameworks,
            requires_browser=requires_browser,
            reasons=reasons,
        )
    
    def quick_check(self, result: FetchResult) -> bool:
        """
        Quick check if browser is needed (without full analysis).
        
        Args:
            result: The FetchResult to check
            
        Returns:
            True if browser is likely needed
        """
        # Very short content likely needs browser
        if len(result.content) < 1000:
            return True
        
        # Check for common SPA indicators
        content = result.content
        spa_indicators = [
            '<div id="root"></div>',
            '<div id="app"></div>',
            '__NEXT_DATA__',
            '_nuxt',
        ]
        
        for indicator in spa_indicators:
            if indicator in content:
                return True
        
        # Check text content
        text = self._extract_text_content(content)
        if len(text) < 200:
            return True
        
        return False
