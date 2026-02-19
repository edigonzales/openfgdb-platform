# ArcGIS Pro 3.x Smokecheck (openfgdb4j / ili2ofgdb)

## Preconditions

- Build artifact contains a generated `.gdb` with domains and relationships.
- ArcGIS Pro 3.x is available.

## Automated Run (2026-02-14)

- Platform: macOS arm64
- Artifact: `/Users/stefan/sources/ili2db-fgdb-codex/openfgdb4j/build/smoke/arcgis-pro-smoke.gdb`
- Backend mode: `OPENFGDB4J_BACKEND=gdal` (default)

### Automated Result Summary

1. Domains created and listed:
- `Enum_Color`
- `Enum_Mode`

2. Domain assignment verified in `GDB_Items.Definition`:
- `obj_a.color -> Enum_Color` (true)
- `obj_a.mode -> Enum_Mode` (true)

3. Relationship classes created and listed:
- `rel_a_b_1to1`
- `rel_a_c_1ton`
- `rel_mn_assoc` (attributed m:n)

4. Cardinality verified in `GDB_Items.Definition`:
- `rel_a_b_1to1 -> esriRelCardinalityOneToOne`
- `rel_a_c_1ton -> esriRelCardinalityOneToMany`
- `rel_mn_assoc -> esriRelCardinalityManyToMany`

5. Idempotency re-apply check:
- Domain count stayed `2`
- Relationship count stayed `3`
- No duplicate names created

## Checklist

1. Open the `.gdb` in ArcGIS Pro Catalog.
2. Verify coded-value domains exist and are visible under Domains.
3. Open a feature class/table field view and verify domain assignment on enum fields.
4. Verify relationship classes exist and can be opened in Relationship Class properties.
5. Verify cardinality and key fields for:
   - `1:1`
   - `1:n`
   - attributed `m:n`
6. Re-run schema import against same `.gdb`.
7. Re-open in ArcGIS Pro and verify no duplicate domains or relationship classes were created.

## Expected Result

- Domains and relationship classes are recognized without catalog errors.
- No duplicate catalog objects after reimport.

## Manual ArcGIS Pro 3.x Gate

- Status: pending manual confirmation in ArcGIS Pro UI
- Suggested file to open: `/Users/stefan/sources/ili2db-fgdb-codex/openfgdb4j/build/smoke/arcgis-pro-smoke.gdb`
