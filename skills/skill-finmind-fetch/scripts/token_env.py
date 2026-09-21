"""Shared FinMind token resolution and round-robin rotation."""
import os

TOKEN_ENV_NAMES = (
    "FINMIND_TOKEN",
    "FINMIND_API_TOKEN",
    "FINDMIND_GMAIL_TOKEN",
    "FINDMIND_GMAIL_TOKEN1",
    "FINDMIND_GMAIL_TOKEN2",
)

def get_finmind_tokens(explicit=None):
    """Return unique configured tokens in priority order, without exposing values."""
    candidates = [explicit] if explicit else [os.getenv(name) for name in TOKEN_ENV_NAMES]
    tokens = []
    for value in candidates:
        if value and value.strip() and value.strip() not in tokens:
            tokens.append(value.strip())
    return tokens

class TokenRotator:
    """Thread-safe enough for the current sequential fetchers; rotates per request."""
    def __init__(self, explicit=None):
        self._tokens = get_finmind_tokens(explicit)
        self._index = 0

    @property
    def count(self):
        return len(self._tokens)

    def next(self):
        if not self._tokens:
            return None
        token = self._tokens[self._index]
        self._index = (self._index + 1) % len(self._tokens)
        return token

    def retire(self, token):
        """Remove a token rejected as illegal, without exposing its value."""
        if token in self._tokens:
            self._tokens.remove(token)
            if self._tokens:
                self._index %= len(self._tokens)
            else:
                self._index = 0
