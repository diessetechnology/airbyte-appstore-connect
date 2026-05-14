from __future__ import annotations

import urllib.parse
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_protocol.models import SyncMode

from source_app_store_connect.auth import AppStoreConnectAuth


class AppStoreConnectStream(HttpStream):
    url_base = "https://api.appstoreconnect.apple.com/v1/"
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__()
        self._config = dict(config)
        self._auth = self._build_auth(self._config)

    @property
    def page_size(self) -> int:
        limit = self._config.get("limit", 200)
        try:
            limit_int = int(limit)
        except Exception:
            limit_int = 200
        return max(1, min(200, limit_int))

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        headers = dict(super().request_headers(stream_state, stream_slice, next_page_token))
        headers["Authorization"] = f"Bearer {self._auth.token()}"
        return headers

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"limit": self.page_size}
        if next_page_token:
            params.update(next_page_token)
        return params

    def next_page_token(self, response, **kwargs) -> Optional[Mapping[str, Any]]:
        body = response.json()
        next_link = (body.get("links") or {}).get("next")
        if not next_link:
            return None

        parsed = urllib.parse.urlparse(next_link)
        query = urllib.parse.parse_qs(parsed.query)
        cursor_values = query.get("cursor") or query.get("page[cursor]")
        if not cursor_values:
            return None
        return {"cursor": cursor_values[0]}

    def parse_response(self, response, **kwargs) -> Iterable[Mapping[str, Any]]:
        body = response.json()
        for record in body.get("data", []) or []:
            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        return {"type": "object", "additionalProperties": True}

    @staticmethod
    def _build_auth(config: Mapping[str, Any]) -> AppStoreConnectAuth:
        return AppStoreConnectAuth(
            issuer_id=config["issuer_id"],
            key_id=config["key_id"],
            private_key=config["private_key"],
        )


class Apps(AppStoreConnectStream):
    name = "apps"

    def path(self, **kwargs) -> str:
        return "apps"


class AppStoreVersions(AppStoreConnectStream):
    name = "app_store_versions"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        for app_id in self._iter_app_ids():
            yield {"app_id": app_id}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        app_id = stream_slice["app_id"]
        return f"apps/{app_id}/appStoreVersions"

    def _iter_app_ids(self) -> Iterable[str]:
        configured = self._config.get("app_ids") or []
        if configured:
            for app_id in configured:
                if app_id:
                    yield str(app_id)
            return

        apps_stream = Apps(self._config)
        for record in apps_stream.read_records(sync_mode=SyncMode.full_refresh):
            app_id = record.get("id")
            if app_id:
                yield str(app_id)


class Builds(AppStoreConnectStream):
    name = "builds"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        for app_id in self._iter_app_ids():
            yield {"app_id": app_id}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        app_id = stream_slice["app_id"]
        return f"apps/{app_id}/builds"

    def _iter_app_ids(self) -> Iterable[str]:
        configured = self._config.get("app_ids") or []
        if configured:
            for app_id in configured:
                if app_id:
                    yield str(app_id)
            return

        apps_stream = Apps(self._config)
        for record in apps_stream.read_records(sync_mode=SyncMode.full_refresh):
            app_id = record.get("id")
            if app_id:
                yield str(app_id)
