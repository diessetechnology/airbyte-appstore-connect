from __future__ import annotations

import csv
import datetime
import gzip
import io
import urllib.parse
from typing import Any, Iterable, Iterator, List, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_protocol.models import SyncMode

from source_app_store_connect.auth import AppStoreConnectAuth


def _parse_yyyy_mm_dd(value: str) -> datetime.date:
    return datetime.date.fromisoformat(value)


def _today_utc() -> datetime.date:
    return datetime.datetime.now(tz=datetime.timezone.utc).date()


def _date_range_inclusive(start: datetime.date, end: datetime.date) -> Iterator[datetime.date]:
    current = start
    while current <= end:
        yield current
        current += datetime.timedelta(days=1)


def _maybe_decompress_gzip(payload: bytes) -> bytes:
    if payload[:2] == b"\x1f\x8b":
        return gzip.decompress(payload)
    return payload


class AppStoreConnectStream(HttpStream):
    url_base = "https://api.appstoreconnect.apple.com/v1/"
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__()
        self._config = dict(config)
        self._auth = self._build_auth(self._config)
        self._raw_session = requests.Session()

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

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        url = urllib.parse.urljoin(self.url_base, path)
        headers = {"Authorization": f"Bearer {self._auth.token()}", "Accept": "application/json"}
        response = self._raw_session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=headers,
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def _iter_json_data(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Iterator[Mapping[str, Any]]:
        current_params = dict(params or {})
        while True:
            body = self._request_json("GET", path, params=current_params)
            for record in body.get("data", []) or []:
                yield record
            next_link = (body.get("links") or {}).get("next")
            if not next_link:
                return
            parsed = urllib.parse.urlparse(next_link)
            query = urllib.parse.parse_qs(parsed.query)
            cursor_values = query.get("cursor") or query.get("page[cursor]")
            if not cursor_values:
                return
            current_params["cursor"] = cursor_values[0]

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
        return super()._iter_app_ids()


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
        return super()._iter_app_ids()


class SalesReports(AppStoreConnectStream):
    name = "sales_reports"
    primary_key = None

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        frequency = str(self._config.get("sales_frequency") or "DAILY").upper()
        if frequency != "DAILY":
            yield {"report_date": self._config.get("sales_start_date") or self._config.get("sales_end_date")}
            return

        end_date = self._config.get("sales_end_date")
        start_date = self._config.get("sales_start_date")
        if not end_date:
            end = _today_utc() - datetime.timedelta(days=1)
        else:
            end = _parse_yyyy_mm_dd(str(end_date))
        if not start_date:
            start = end
        else:
            start = _parse_yyyy_mm_dd(str(start_date))

        for d in _date_range_inclusive(start, end):
            yield {"report_date": d.isoformat()}

    def path(self, **kwargs) -> str:
        return "salesReports"

    def next_page_token(self, response, **kwargs) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        vendor_number = self._config.get("vendor_number")
        if not vendor_number:
            raise ValueError("vendor_number è richiesto per scaricare /v1/salesReports")

        params: MutableMapping[str, Any] = {
            "filter[frequency]": str(self._config.get("sales_frequency") or "DAILY").upper(),
            "filter[reportType]": str(self._config.get("sales_report_type") or "SALES").upper(),
            "filter[reportSubType]": str(self._config.get("sales_report_sub_type") or "SUMMARY").upper(),
            "filter[vendorNumber]": str(vendor_number),
        }

        report_date = (stream_slice or {}).get("report_date")
        if report_date:
            params["filter[reportDate]"] = str(report_date)

        version = self._config.get("sales_version")
        if version:
            params["filter[version]"] = str(version)

        return params

    def parse_response(self, response, **kwargs) -> Iterable[Mapping[str, Any]]:
        payload = _maybe_decompress_gzip(response.content)
        text = payload.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")

        report_date = None
        stream_slice = kwargs.get("stream_slice") or {}
        if isinstance(stream_slice, Mapping):
            report_date = stream_slice.get("report_date")

        for row in reader:
            if not row:
                continue
            if not any((v or "").strip() for v in row.values()):
                continue
            out = dict(row)
            out["_meta_vendor_number"] = str(self._config.get("vendor_number"))
            out["_meta_frequency"] = str(self._config.get("sales_frequency") or "DAILY").upper()
            out["_meta_report_type"] = str(self._config.get("sales_report_type") or "SALES").upper()
            out["_meta_report_sub_type"] = str(self._config.get("sales_report_sub_type") or "SUMMARY").upper()
            out["_meta_report_date"] = report_date
            yield out


class AnalyticsReportRequests(AppStoreConnectStream):
    name = "analytics_report_requests"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        for app_id in self._iter_app_ids():
            yield {"app_id": app_id}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"apps/{stream_slice['app_id']}/analyticsReportRequests"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        access_type = self._config.get("analytics_access_type")
        if access_type:
            params["filter[accessType]"] = str(access_type).upper()
        return params


class AnalyticsReports(AppStoreConnectStream):
    name = "analytics_reports"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        access_type = str(self._config.get("analytics_access_type") or "ONGOING").upper()
        categories = [str(x) for x in (self._config.get("analytics_categories") or []) if x]
        names = [str(x) for x in (self._config.get("analytics_report_names") or []) if x]
        category_values = categories if categories else [None]
        name_values = names if names else [None]
        for app_id in self._iter_app_ids():
            params = {"filter[accessType]": access_type, "limit": 200}
            requests_data = list(self._iter_json_data(f"apps/{app_id}/analyticsReportRequests", params))
            for request_record in requests_data:
                request_id = request_record.get("id")
                if request_id:
                    for category in category_values:
                        for name in name_values:
                            yield {
                                "app_id": app_id,
                                "request_id": str(request_id),
                                "category": category,
                                "name": name,
                            }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"analyticsReportRequests/{stream_slice['request_id']}/reports"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        if stream_slice:
            category = stream_slice.get("category")
            name = stream_slice.get("name")
            if category:
                params["filter[category]"] = str(category)
            if name:
                params["filter[name]"] = str(name)
        return params


class AnalyticsReportInstances(AppStoreConnectStream):
    name = "analytics_report_instances"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        access_type = str(self._config.get("analytics_access_type") or "ONGOING").upper()
        reports = AnalyticsReports(self._config)
        seen: set[str] = set()
        for report in reports.read_records(sync_mode=SyncMode.full_refresh):
            report_id = report.get("id")
            if report_id:
                report_id_str = str(report_id)
                if report_id_str in seen:
                    continue
                seen.add(report_id_str)
                yield {"report_id": report_id_str, "access_type": access_type}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"analyticsReports/{stream_slice['report_id']}/instances"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        granularity = self._config.get("analytics_granularity")
        if granularity:
            params["filter[granularity]"] = str(granularity).upper()
        return params


class AnalyticsReportSegments(AppStoreConnectStream):
    name = "analytics_report_segments"

    def stream_slices(
        self,
        sync_mode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        instances = AnalyticsReportInstances(self._config)
        for instance in instances.read_records(sync_mode=SyncMode.full_refresh):
            instance_id = instance.get("id")
            if instance_id:
                yield {"instance_id": str(instance_id)}

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"analyticsReportInstances/{stream_slice['instance_id']}/segments"


class AnalyticsReportSegmentRows(AppStoreConnectStream):
    name = "analytics_report_rows"
    primary_key = None

    def path(self, **kwargs) -> str:
        return ""

    def next_page_token(self, response, **kwargs) -> Optional[Mapping[str, Any]]:
        return None

    def read_records(
        self,
        sync_mode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        access_type = str(self._config.get("analytics_access_type") or "ONGOING").upper()
        granularity = str(self._config.get("analytics_granularity") or "DAILY").upper()

        start_date_raw = self._config.get("analytics_start_date")
        end_date_raw = self._config.get("analytics_end_date")
        if end_date_raw:
            end_date = _parse_yyyy_mm_dd(str(end_date_raw))
        else:
            end_date = _today_utc()
        if start_date_raw:
            start_date = _parse_yyyy_mm_dd(str(start_date_raw))
        else:
            start_date = end_date - datetime.timedelta(days=7)

        categories = [str(x) for x in (self._config.get("analytics_categories") or []) if x]
        names = [str(x) for x in (self._config.get("analytics_report_names") or []) if x]
        auto_create = bool(self._config.get("analytics_auto_create_requests", True))
        category_values = categories if categories else [None]
        name_values = names if names else [None]

        for app_id in self._iter_app_ids():
            request_id = self._get_or_create_analytics_request_id(app_id, access_type, auto_create)
            if not request_id:
                continue

            for category in category_values:
                for name in name_values:
                    report_params: MutableMapping[str, Any] = {"limit": 200}
                    if category:
                        report_params["filter[category]"] = category
                    if name:
                        report_params["filter[name]"] = name

                    seen_reports: set[str] = set()
                    for report in self._iter_json_data(f"analyticsReportRequests/{request_id}/reports", report_params):
                        report_id_value = report.get("id")
                        if not report_id_value:
                            continue
                        report_id = str(report_id_value)
                        if report_id in seen_reports:
                            continue
                        seen_reports.add(report_id)

                        report_name = (
                            (report.get("attributes") or {}).get("name") if isinstance(report.get("attributes"), Mapping) else None
                        )
                        report_category = (
                            (report.get("attributes") or {}).get("category") if isinstance(report.get("attributes"), Mapping) else None
                        )

                        instances_params: MutableMapping[str, Any] = {"limit": 200, "filter[granularity]": granularity}
                        for instance in self._iter_json_data(f"analyticsReports/{report_id}/instances", instances_params):
                            attrs = instance.get("attributes") if isinstance(instance.get("attributes"), Mapping) else {}
                            processing_date = attrs.get("processingDate")
                            if processing_date:
                                try:
                                    pd = _parse_yyyy_mm_dd(str(processing_date))
                                except Exception:
                                    pd = None
                                if pd and (pd < start_date or pd > end_date):
                                    continue

                            instance_id_value = instance.get("id")
                            if not instance_id_value:
                                continue
                            instance_id = str(instance_id_value)
                            for segment in self._iter_json_data(
                                f"analyticsReportInstances/{instance_id}/segments", {"limit": 200}
                            ):
                                seg_attrs = segment.get("attributes") if isinstance(segment.get("attributes"), Mapping) else {}
                                url = seg_attrs.get("url")
                                if not url:
                                    continue
                                segment_id = str(segment.get("id"))

                                for row in self._download_and_parse_segment_csv(url):
                                    out = dict(row)
                                    out["_meta_app_id"] = app_id
                                    out["_meta_access_type"] = access_type
                                    out["_meta_request_id"] = request_id
                                    out["_meta_report_id"] = report_id
                                    out["_meta_report_name"] = report_name
                                    out["_meta_report_category"] = report_category
                                    out["_meta_instance_id"] = instance_id
                                    out["_meta_processing_date"] = processing_date
                                    out["_meta_granularity"] = granularity
                                    out["_meta_segment_id"] = segment_id
                                    yield out

    def _get_or_create_analytics_request_id(self, app_id: str, access_type: str, auto_create: bool) -> Optional[str]:
        params = {"filter[accessType]": access_type, "limit": 200}
        existing = list(self._iter_json_data(f"apps/{app_id}/analyticsReportRequests", params))
        if existing:
            request_id = existing[0].get("id")
            return str(request_id) if request_id else None

        if not auto_create:
            return None

        body = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": access_type},
                "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
            }
        }
        created = self._request_json("POST", "analyticsReportRequests", json_body=body)
        created_id = (created.get("data") or {}).get("id") if isinstance(created.get("data"), Mapping) else None
        return str(created_id) if created_id else None

    def _download_and_parse_segment_csv(self, url: str) -> Iterable[Mapping[str, Any]]:
        response = self._raw_session.get(url, timeout=120)
        response.raise_for_status()
        payload = _maybe_decompress_gzip(response.content)
        text = payload.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            if not row:
                continue
            if not any((v or "").strip() for v in row.values()):
                continue
            yield row
