# Load a Data Vault link

At the end of this guide a link records the relationships between two hubs, here orders and
products: one row per relationship ever seen, never updated or deleted, loaded from one source
table by a Declarative Query Definition (DQD). It is for a user of the library who already loads
hubs and satellites and now connects them. The steps are the same on every platform.

## Before you start

- You can load a hub and a satellite; see [Load a Data Vault entity](load-a-data-vault-entity.md).
  This guide uses its `spark`, and `platform="fabric"`.
- Each hub the link connects is loaded from a source at its own key's grain: every order once in
  the orders source, every product once in the products source. The library does not
  de-duplicate staged keys. A source at a finer grain, such as order lines read for the order
  hub, stages one order once per line and is refused; producing unique keys is the source's
  business logic.
- The link's source holds each relationship once: each order and product pair in one row. Two
  lines for the same order and product are one relationship twice, and are refused. Decide the
  grain upstream, by aggregating the lines or by adding the line number to the link's key.

## Steps

1. **Build the link hash key and the hub hash keys in the DQD.** The link hash key is the hash of
   the hubs' business keys, each normalised as its hub's key is (cast to string, NULL to the empty
   string, trimmed, upper-cased), joined by `||` in a fixed order; each hub hash key is built
   exactly as in that hub's load context, the canonical expression of
   [Load a Data Vault entity](load-a-data-vault-entity.md), step 1. Use the same expressions
   in every load context that builds them.

   ```json
   [
     {"name": "OrderHashKey", "expression": {"function": "sha2", "arguments": [
       {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "ORDER_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
       {"literal": 256}]}},
     {"name": "ProductHashKey", "expression": {"function": "sha2", "arguments": [
       {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "PRODUCT_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
       {"literal": 256}]}},
     {"name": "LinkHashKey", "expression": {"function": "sha2", "arguments": [
       {"function": "concat_ws", "arguments": [
         {"literal": "||"},
         {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "ORDER_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
         {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "PRODUCT_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]}]},
       {"literal": 256}]}}
   ]
   ```

   The order of the business keys is part of the key: the same pair joined the other way round is
   another link key, and nothing can detect it.

2. **Write the link's load context.** `scd_type` is `dv_link`, `business_key_column` is the link
   hash key, and `link_hub_key_columns` names the hub hash key columns, at least two. Select the
   link hash key and the hub hash keys; the business keys stay in the hubs.

   ```json
   {
     "global_context": {
       "id": "ent_1",
       "entity_name": "OrderProduct",
       "source_system": "CRM",
       "target_table_path": "/lake/vault/order_product_l",
       "staging_table_path": "/lake/staging/order_lines"
     },
     "scd_context": {
       "scd_type": "dv_link",
       "business_key_column": "LinkHashKey",
       "link_hub_key_columns": ["OrderHashKey", "ProductHashKey"]
     },
     "dqd_context": {
       "source_kind": "delta",
       "source_identifier": "/lake/bronze/order_lines",
       "transformations": [
         {"name": "OrderHashKey", "expression": {"function": "sha2", "arguments": [
           {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "ORDER_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
           {"literal": 256}]}},
         {"name": "ProductHashKey", "expression": {"function": "sha2", "arguments": [
           {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "PRODUCT_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
           {"literal": 256}]}},
         {"name": "LinkHashKey", "expression": {"function": "sha2", "arguments": [
           {"function": "concat_ws", "arguments": [
             {"literal": "||"},
             {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "ORDER_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]},
             {"function": "upper", "arguments": [{"function": "trim", "arguments": [{"function": "coalesce", "arguments": [{"cast": {"expression": {"column": "PRODUCT_ID"}, "data_type": "string"}}, {"literal": ""}]}]}]}]},
           {"literal": 256}]}}
       ],
       "selection": [{"name": "LinkHashKey"}, {"name": "OrderHashKey"}, {"name": "ProductHashKey"}]
     },
     "runtime_context": {"correlation_id": "vault-1", "pipeline_name": "order_product_l", "application_name": "crm"},
     "logging_context": {"logging_level": "INFO", "log_file_name": "order_product_l", "log_file_dir": "/lake/logs"}
   }
   ```

3. **Run the hubs, then the link.** Put the two hubs' load contexts in `order_hub_context` and
   `product_hub_context`, and step 2's JSON in `link_context`. Loading the hubs first keeps every
   link pointing at a hub row. The library does not check that a link's hub keys exist in their
   hubs: a link relies on run order, so load every hub a link references before the link, or
   earlier in the same orchestration. A key with no hub row is loaded as it is, an orphan, and
   nothing reports it.

   ```python
   from spark_quick_start.orchestration import dv_hub_pipeline, dv_link_pipeline

   for hub_context in (order_hub_context, product_hub_context):
       dv_hub_pipeline(hub_context, spark, platform="fabric").run()
   link = dv_link_pipeline(link_context, spark, platform="fabric").run()
   ```

4. **Track the relationship's attributes in a link satellite, if it has any.** A link satellite is
   a satellite whose `business_key_column` is the link hash key: write its load context as in
   [Load a Data Vault entity](load-a-data-vault-entity.md), step 3, with `LinkHashKey` and the
   attributes selected, and load it after the link with `dv_satellite_pipeline`.

5. **Give an optional relationship its hub row.** With the canonical expression, a missing
   business key is cast and coalesced to the empty string before it is hashed, so the
   relationship gets a fixed hub hash key, the hash of `""`, rather than NULL. Write one row with
   that key to the hub once, so the link has a hub row to point at. A hub key that is still NULL
   or empty, because it was built without `coalesce`, is refused.

## Check it worked

`link.status` is `"success"`, `link.metrics["inserted_count"]` is the number of relationships the
link had not seen, and `updated_count` is `0`, always.

```python
print(link.status, link.metrics["inserted_count"], link.metrics["updated_count"])
```

Measured locally with order lines o1–p1 and o1–p2: the first run inserted 2 relationships and
the rerun 0; a second source with o1–p2 again and a new o2–p1 inserted 1. A link satellite on the
lines' quantity took a new row when one quantity changed.

## When it goes wrong

A load's error arrives as `result.status == "error"`, with the message in
`error_details["error_message"]`; nothing has been written.

**`… key(s) staged more than once` on a hub.** The hub reads a source at a finer grain than its
key, such as order lines for the order hub. Give the hub a source with each key once.

**`… key(s) staged more than once` on the link.** The source holds one relationship in two rows.
Decide the grain upstream.

**`… row(s) with a NULL or empty hub key in ['OrderHashKey', 'ProductHashKey']`.** A hub hash key
was built without the canonical `coalesce`, and a business key is missing. Build it as in step 1
(step 5 says what a missing key then becomes).

**`… has rows but no hash seal, so the settings its hash diff was computed with are unknown`.**
The table was written outside this library, or a load stopped between its first write and the
seal. Find the hash settings its rows were written with, seal them deliberately with the adapter's
`set_table_properties`, then rerun.

**`Refusing to create a Delta table at '…': the path already exists and holds files, but is not a
Delta table`.** The table did not exist, so the writer creates it with `write_delta_create`, and
the path in the load context holds other files: usually a mistyped or copied path. Nothing was
written; correct the path.

**`scd_context.link_hub_key_columns must name at least two staged columns`.** The key is missing
from a `dv_link` load context, or names one column.

**`scd_context.link_hub_key_columns is a dv_link key, but scd_type is …`.** The key was set in a
hub's or satellite's load context; remove it there.

## See also

- [Orchestrator reference](../reference/orchestrator.md#dv_link_pipeline): what `dv_link_pipeline`
  composes.
- [Load-context reference](../reference/load-context.md): `link_hub_key_columns` and the other Data
  Vault keys of `scd_context`.
- [The Data Vault link design](../architecture/decisions/2026-10-01-data-vault-link-design.md):
  why links carry no referential check, and what is left for later.
