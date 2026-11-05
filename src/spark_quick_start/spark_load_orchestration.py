"""
Compatibility shim — re-exports from the new module locations.

The original spark_load_orchestration module was removed during the Gen3
refactor. This shim restores the old import path so existing staging notebooks
require no changes:

    from spark_quick_start.spark_load_orchestration import create_staging_main_function
"""
from spark_quick_start.orchestration.staging_pipeline import (
    create_staging_main_function,
)

__all__ = ["create_staging_main_function"]
