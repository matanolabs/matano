import os
import time
import asyncio
import logging
import aiobotocore
import polars as pl
import yaml
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _load_enrichment_configs():
    ret = {}
    path = Path("/opt/config/enrichment")
    enrichment_configs = path.rglob("enrichment_table.yml")
    for enrichment_conf in enrichment_configs:
        with enrichment_conf.open() as f:
            conf = yaml.safe_load(f)
            ret[conf['name']] = conf
    return ret

class Enrichment:
    def __init__(self, s3, table_configs, event_loop) -> None:
        self._tables = {}
        self._table_configs = table_configs
        self._enrichment_tables_bucket = os.environ["ENRICHMENT_TABLES_BUCKET"]
        self._s3 = s3
        self._event_loop = event_loop
        self._load_task = self._event_loop.create_task(self._load_tables())
        self._loaded = False
        self._executor = ThreadPoolExecutor(1)

    def _load_tables_data(self, responses: list):
        for (table_name, data) in zip(self._table_configs.keys(), responses):
            df = pl.read_parquet(BytesIO(data))
            if (primary_key := self._get_table_primary_key(table_name)):
                df = df.sort(primary_key)
            self._tables[table_name] = df

    async def _download_table(self, table_name):
        fut = self._s3.get_object(
            Bucket=self._enrichment_tables_bucket,
            Key=f"tables/{table_name}.parquet"
        )
        resp = await fut
        return await resp["Body"].read()

    async def _load_tables(self):
        if not isinstance(self._s3, aiobotocore.client.BaseClient):
            self._s3 = await self._s3.__aenter__()
        futs = []
        for table_name in self._table_configs:
            futs.append(self._download_table(table_name))

        composite_fut = asyncio.gather(*futs)
        responses = await composite_fut
        # separate CPU code from IO
        await self._event_loop.run_in_executor(self._executor, self._load_tables_data, responses)
        self._loaded = True
        logger.info("Loaded enrichment tables.")

    def _ensure_loaded(self):
        if not self._loaded:
            self._event_loop.run_until_complete(self._load_task)

    def _get_table_primary_key(self, table_name: str):
        return self._table_configs[table_name]["schema"].get("primary_key")

    def get_record_by_key(self, table_name: str, key: str):
        self._ensure_loaded()
        table: pl.DataFrame = self._tables[table_name]
        pk_col = self._get_table_primary_key(table_name)
        rows = table.lazy().filter(pl.col(pk_col) == key).limit(1).collect().to_dicts()
        return next(iter(rows), None)
