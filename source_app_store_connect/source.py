from __future__ import annotations

import json
import os
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import ConnectorSpecification
from airbyte_protocol.models import SyncMode

from source_app_store_connect.streams import AppStoreVersions, Apps, Builds


class SourceAppStoreConnect(AbstractSource):
    def spec(self, logger) -> ConnectorSpecification:
        spec_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "spec.json"))
        with open(spec_path, "r", encoding="utf-8") as f:
            return ConnectorSpecification(**json.load(f))

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            stream = Apps(config)
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records, None)
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Apps(config), AppStoreVersions(config), Builds(config)]
