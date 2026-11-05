from spark_quick_start.orchestration.column_policy import ColumnNamingPolicy
from spark_quick_start.orchestration.models import SCDContext

_BASE = {"business_key_column": "entityKey"}


def test_default_policy_applied_when_no_policy_given():
    ctx = SCDContext(_BASE)
    assert ctx.row_key_column == "scdRowKey"
    assert ctx.hash_column == "scdHash"
    assert ctx.version_column == "scdVersion"
    assert ctx.effective_from_column == "scdEffectiveFrom"
    assert ctx.effective_to_column == "scdEffectiveTo"
    assert ctx.current_flag_column == "scdCurrentFlag"
    assert ctx.deleted_flag_column == "scdDeletedFlag"
    assert ctx.source_system_column == "scdSourceSystem"
    assert ctx.correlation_id_column == "scdCorrelationId"


def test_custom_policy_applied():
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
    ctx = SCDContext(_BASE, policy=policy)
    assert ctx.row_key_column == "natKey"
    assert ctx.hash_column == "natHash"
    assert ctx.version_column == "natVersion"
    assert ctx.effective_from_column == "validFrom"
    assert ctx.effective_to_column == "validTo"
    assert ctx.current_flag_column == "isCurrent"
    assert ctx.deleted_flag_column == "isDeleted"
    assert ctx.source_system_column == "sourceSystem"
    assert ctx.correlation_id_column == "correlationId"


def test_per_column_json_override_takes_precedence_over_policy():
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
    data = {**_BASE, "hash_column": "myHash"}
    ctx = SCDContext(data, policy=policy)
    assert ctx.hash_column == "myHash"        # JSON override wins
    assert ctx.row_key_column == "natKey"     # rest comes from policy
    assert ctx.version_column == "natVersion"


def test_no_policy_no_json_override_uses_scd_defaults():
    ctx = SCDContext(_BASE)
    assert ctx.hash_column == "scdHash"
    assert ctx.row_key_column == "scdRowKey"


def test_source_deleted_flag_column_defaults_to_none():
    ctx = SCDContext({"business_key_column": "id"})
    assert ctx.source_deleted_flag_column is None


def test_source_deleted_flag_column_set_from_json():
    ctx = SCDContext({"business_key_column": "id", "source_deleted_flag_column": "is_deleted"})
    assert ctx.source_deleted_flag_column == "is_deleted"


def test_source_deleted_flag_column_empty_string_becomes_none():
    ctx = SCDContext({"business_key_column": "id", "source_deleted_flag_column": ""})
    assert ctx.source_deleted_flag_column is None
