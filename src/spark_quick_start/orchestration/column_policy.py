"""
column_policy.py — ColumnNamingPolicy: grouped SCD meta column name configuration.

Consumers instantiate a policy once and pass it to SCDContext to set all nine
meta column names in one step, rather than overriding each via JSON config.
Per-column JSON overrides in SCDContext always take precedence over the policy.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnNamingPolicy:
    """Grouped naming policy for the nine SCD meta columns injected by the pipeline.

    Every field is an SCD meta column, and ``_scd1_update_columns`` never updates a column with
    that name, so a name that only non-SCD styles write belongs in ``Constants`` (the Data Vault
    load-audit names are ``Constants.DV_*``).

    Use ``ColumnNamingPolicy.default()`` for the built-in ``scd*`` convention, or
    construct directly for custom naming::

        policy = ColumnNamingPolicy(
            row_key_column="natKey",
            hash_column="natHash",
            version_column="natVersion",
            effective_from_column="validFrom",
            effective_to_column="validTo",
            current_flag_column="isCurrent",
            deleted_flag_column="isDeleted",
            source_system_column="sourceSystem",
            correlation_id_column="correlationId",
        )
    """

    row_key_column: str
    hash_column: str
    version_column: str
    effective_from_column: str
    effective_to_column: str
    current_flag_column: str
    deleted_flag_column: str
    source_system_column: str
    correlation_id_column: str

    @classmethod
    def default(cls) -> ColumnNamingPolicy:
        """Return the built-in ``scd*`` naming convention."""
        return cls(
            row_key_column="scdRowKey",
            hash_column="scdHash",
            version_column="scdVersion",
            effective_from_column="scdEffectiveFrom",
            effective_to_column="scdEffectiveTo",
            current_flag_column="scdCurrentFlag",
            deleted_flag_column="scdDeletedFlag",
            source_system_column="scdSourceSystem",
            correlation_id_column="scdCorrelationId",
        )

    @classmethod
    def data_vault(cls) -> ColumnNamingPolicy:
        """Return the Data Vault 2.0 names for a hub or a satellite (F156).

        The version, effective-date and current-flag columns are not written by a Data Vault
        style; they keep their SCD names so the meta-column checks still see them.
        """
        return cls(
            row_key_column="HashKey",
            hash_column="HashDiff",
            version_column="scdVersion",
            effective_from_column="scdEffectiveFrom",
            effective_to_column="scdEffectiveTo",
            current_flag_column="scdCurrentFlag",
            deleted_flag_column="IsDeleted",
            source_system_column="RecordSource",
            correlation_id_column="CorrelationId",
        )
