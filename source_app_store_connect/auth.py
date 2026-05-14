from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import jwt


def _normalize_p8_private_key(private_key: str) -> str:
    if "\\n" in private_key and "\n" not in private_key:
        return private_key.replace("\\n", "\n")
    return private_key


@dataclass
class AppStoreConnectAuth:
    issuer_id: str
    key_id: str
    private_key: str

    _cached_token: Optional[str] = None
    _cached_exp: int = 0

    def token(self) -> str:
        now = int(time.time())
        if self._cached_token and (self._cached_exp - now) > 60:
            return self._cached_token

        exp = now + (20 * 60)
        headers = {"kid": self.key_id, "typ": "JWT", "alg": "ES256"}
        payload = {"iss": self.issuer_id, "exp": exp, "aud": "appstoreconnect-v1"}

        token = jwt.encode(
            payload=payload,
            key=_normalize_p8_private_key(self.private_key),
            algorithm="ES256",
            headers=headers,
        )
        if isinstance(token, bytes):
            token = token.decode("utf-8")

        self._cached_token = token
        self._cached_exp = exp
        return token
