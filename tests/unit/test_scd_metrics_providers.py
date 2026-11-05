"""Every shipped SCDMetricsProvider reports no counts until it has run (F117)."""
from __future__ import annotations

import pytest

from spark_quick_start.orchestration.transformers.dv_hub import DVHubWriterTransformer
from spark_quick_start.orchestration.transformers.dv_link import DVLinkWriterTransformer
from spark_quick_start.orchestration.transformers.dv_satellite import DVSatelliteWriterTransformer
from spark_quick_start.orchestration.transformers.native_scd import (
    NativeSCDAppendOnlyTransformer,
    NativeSCDMergeTransformer,
)
from spark_quick_start.orchestration.transformers.protocols import SCDMetricsProvider


@pytest.mark.parametrize("cls", [
    NativeSCDMergeTransformer, NativeSCDAppendOnlyTransformer,
    DVHubWriterTransformer, DVLinkWriterTransformer, DVSatelliteWriterTransformer,
])
def test_counts_are_none_until_the_writer_runs(cls):
    writer = cls()
    assert isinstance(writer, SCDMetricsProvider)
    assert (writer.inserted, writer.updated) == (None, None)
