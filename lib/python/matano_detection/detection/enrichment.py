import yaml
import logging
from pathlib import Path
from detection_lib import create_enrichment_tables

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def _load_enrichment_configs():
    ret = {}
    path = Path("/opt/config/enrichment")
    enrichment_configs = path.rglob("enrichment.yml")
    for enrichment_conf in enrichment_configs:
        with enrichment_conf.open() as f:
            conf = yaml.safe_load(f)
            ret[conf['name']] = conf
    return ret


def _load_enrich_configs():
    enrichment_configs = _load_enrichment_configs()
    ret = []
    for name, conf in enrichment_configs.items():
        pk = conf.get("schema", {}).get("primary_key")
        ret.append((name, pk))
    return ret

def _load_enrichment_tables(module):
    tables = create_enrichment_tables(_load_enrich_configs())
    names = [table.name for table in tables]
    for table in tables:
        setattr(module, table.name, table)
    logger.info(f"Loaded enrichment tables: {names}")
