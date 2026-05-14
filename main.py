import sys

from airbyte_cdk.entrypoint import launch

from source_app_store_connect.source import SourceAppStoreConnect


def run() -> None:
    source = SourceAppStoreConnect()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
