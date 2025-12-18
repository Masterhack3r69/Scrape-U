"""
User-Agent Rotator Module

Rotates User-Agent strings with matching Client Hints headers to avoid detection.
Always use modern, real browser fingerprints - never use default Python-Requests agent.
"""

import random
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class BrowserProfile:
    """A complete browser fingerprint with matching headers."""
    
    user_agent: str
    sec_ch_ua: str
    sec_ch_ua_mobile: str
    sec_ch_ua_platform: str
    accept_language: str = "en-US,en;q=0.9"
    accept_encoding: str = "gzip, deflate, br"


# Curated list of modern browser profiles (updated Dec 2024)
BROWSER_PROFILES = [
    # Chrome on Windows
    BrowserProfile(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        sec_ch_ua_mobile="?0",
        sec_ch_ua_platform='"Windows"',
    ),
    BrowserProfile(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="119", "Google Chrome";v="119"',
        sec_ch_ua_mobile="?0",
        sec_ch_ua_platform='"Windows"',
    ),
    # Chrome on macOS
    BrowserProfile(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        sec_ch_ua_mobile="?0",
        sec_ch_ua_platform='"macOS"',
    ),
    # Firefox on Windows
    BrowserProfile(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        sec_ch_ua="",  # Firefox doesn't send Client Hints
        sec_ch_ua_mobile="",
        sec_ch_ua_platform="",
    ),
    BrowserProfile(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        sec_ch_ua="",
        sec_ch_ua_mobile="",
        sec_ch_ua_platform="",
    ),
    # Firefox on macOS
    BrowserProfile(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
        sec_ch_ua="",
        sec_ch_ua_mobile="",
        sec_ch_ua_platform="",
    ),
    # Edge on Windows
    BrowserProfile(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"',
        sec_ch_ua_mobile="?0",
        sec_ch_ua_platform='"Windows"',
    ),
    # Safari on macOS
    BrowserProfile(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
        sec_ch_ua="",  # Safari doesn't send Client Hints
        sec_ch_ua_mobile="",
        sec_ch_ua_platform="",
    ),
    # Chrome on Android (Mobile)
    BrowserProfile(
        user_agent="Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        sec_ch_ua='"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        sec_ch_ua_mobile="?1",
        sec_ch_ua_platform='"Android"',
    ),
    # Safari on iPhone (Mobile)
    BrowserProfile(
        user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
        sec_ch_ua="",
        sec_ch_ua_mobile="",
        sec_ch_ua_platform="",
    ),
]


class UserAgentRotator:
    """
    Rotates User-Agent strings with matching Client Hints.
    
    Features:
    - Curated list of modern browser fingerprints
    - Matching Sec-Ch-Ua headers to avoid detection
    - Random or sequential rotation strategies
    - Custom profile support
    
    Example:
        rotator = UserAgentRotator()
        headers = rotator.get_headers()
        # Use headers in your request
    """
    
    def __init__(
        self,
        profiles: list[BrowserProfile] | None = None,
        include_mobile: bool = True,
    ):
        """
        Initialize the rotator.
        
        Args:
            profiles: Custom browser profiles (uses defaults if None)
            include_mobile: Whether to include mobile browsers
        """
        if profiles is not None:
            self._profiles = profiles
        elif include_mobile:
            self._profiles = BROWSER_PROFILES.copy()
        else:
            # Filter out mobile profiles
            self._profiles = [
                p for p in BROWSER_PROFILES
                if p.sec_ch_ua_mobile != "?1" and "Mobile" not in p.user_agent
            ]
        
        self._current_index = 0
    
    def get_random_profile(self) -> BrowserProfile:
        """Get a random browser profile."""
        return random.choice(self._profiles)
    
    def get_next_profile(self) -> BrowserProfile:
        """Get the next profile in rotation (round-robin)."""
        profile = self._profiles[self._current_index]
        self._current_index = (self._current_index + 1) % len(self._profiles)
        return profile
    
    def get_headers(self, random_selection: bool = True) -> Dict[str, str]:
        """
        Get a complete set of headers for a request.
        
        Args:
            random_selection: If True, random profile; if False, round-robin
            
        Returns:
            Dict of HTTP headers
        """
        profile = self.get_random_profile() if random_selection else self.get_next_profile()
        
        headers = {
            "User-Agent": profile.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": profile.accept_language,
            # Note: Do NOT set Accept-Encoding manually - httpx handles decompression automatically
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # Add Client Hints if present (Chrome/Edge only)
        if profile.sec_ch_ua:
            headers["Sec-Ch-Ua"] = profile.sec_ch_ua
            headers["Sec-Ch-Ua-Mobile"] = profile.sec_ch_ua_mobile
            headers["Sec-Ch-Ua-Platform"] = profile.sec_ch_ua_platform
            headers["Sec-Fetch-Dest"] = "document"
            headers["Sec-Fetch-Mode"] = "navigate"
            headers["Sec-Fetch-Site"] = "none"
            headers["Sec-Fetch-User"] = "?1"
        
        return headers
    
    def get_user_agent(self) -> str:
        """Get just the User-Agent string (for simple use cases)."""
        return self.get_random_profile().user_agent
    
    def add_profile(self, profile: BrowserProfile) -> None:
        """Add a custom browser profile."""
        self._profiles.append(profile)
    
    @property
    def profile_count(self) -> int:
        """Number of available profiles."""
        return len(self._profiles)
