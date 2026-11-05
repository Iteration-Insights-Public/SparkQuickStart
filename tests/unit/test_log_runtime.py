"""Tests for load_pipeline.log_runtime()."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from spark_quick_start.orchestration.load_pipeline import log_runtime


def _spark_with_conf(values: dict) -> MagicMock:
    spark = MagicMock()
    spark.conf.get.side_effect = lambda key: values.get(key)
    spark.sparkContext.applicationId = "app-1"
    spark.sparkContext.appName = "test-app"
    return spark


def test_log_runtime_includes_aqe_and_shuffle_config():
    spark = _spark_with_conf({
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
    })
    logger = MagicMock()

    log_runtime(spark, "pre-staging", logger)

    payload = json.loads(logger.info.call_args[0][0])
    assert payload["adaptive_enabled"] == "true"
    assert payload["adaptive_coalesce_partitions_enabled"] == "true"
    assert payload["shuffle_partitions"] == "200"
