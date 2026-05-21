# F5 — Typed Columnar Shredding for the JSON Index

> Status: design doc + minimal POC.
> Scope: storage format, query routing, mutable-segment story, nested-array
> handling, schema discovery, migration. The POC at the end of this branch
> proves only the *projection routing* slice.

## Executive summary

Pinot's JSON index today is a single inverted index over a flattened key-value
representation of a JSON document. It is universal but pays a heavy tax on
analytic shapes: numeric values are compared as UTF-8 (precision loss on
`RANGE`), there is no per-path forward index (no aggregation pushdown past
`COUNT`), and cross-array unnest can explode at ingest. ClickHouse 24.10's
typed JSON column, Snowflake's VARIANT, and Parquet/Dremel have all converged
on the same answer: **shred frequent paths into their own typed, columnar
storage with min/max statistics**, and keep an inverted/variant fallback for
the long tail. This document specifies how F5 brings that architecture into
Pinot — an additive layer on top of today's JSON index that produces per-path
forward indexes (and optionally per-path range / null indexes) and routes
matching projections and predicates to them. The POC in this branch lands the
config surface, the shred-creation hook, and projection-side routing for
`jsonExtractScalar(col, '$.x', T)`. Predicate pushdown, range index, schema
discovery, and migration are explicitly out of POC scope and called out
under future work.

---

## 1. Motivation

Three query shapes that lose today, with traces from the current code path:

| Query | Current behavior | With typed shredding |
|---|---|---|
| `WHERE JSON_MATCH(col, '"$.ts" > 1700000000000')` | `ImmutableJsonIndexReader.getMatchingFlattenedDocIdsForKeyValue` (`RANGE` case) scans every dictionary entry for `.ts.*`, parses each string to a `DOUBLE` (`rangeDataType = DOUBLE`), compares, and OR-merges per-value posting lists. Cost ~ O(distinct values × dict entry size + posting list size). Numeric precision capped at `DOUBLE`, so `LONG` epoch-millis lose 3 LSBs near `2^53`. | Read the `$.ts` shred as a packed LONG forward index. Bound the scan with the per-segment / per-block min/max footer. For point-in-time aggregations (`MIN(ts)`, `MAX(ts)`), the footer alone answers the query. |
| `SELECT MIN(jsonExtractScalar(col, '$.lat', 'DOUBLE')) FROM t WHERE …` | `JsonExtractScalarTransformFunction.transformToDoubleValuesSV` parses the raw JSON column for every doc that survives filtering, runs `JsonPath` against a freshly-parsed Jackson tree, and coerces. Cost dominated by per-row Jackson parse (~1 µs per row on modern hardware). | Read 8-byte doubles directly from the `$.lat` shred. ~30 ns/row, zero allocation. |
| `SELECT region, SUM(jsonExtractScalar(col, '$.revenue', 'DOUBLE')) FROM t GROUP BY region` | Per-row Jackson parse + coerce on the projection side; the JSON index sees no aggregation pushdown — only `JSON_MATCH` filter pushdown. | Directly drive `SumAggregationFunction` from the `$.revenue` forward-index reader. Standard `SumValueAggregator` fast path applies; no change to aggregation code. |

**Industry numbers** (from prior knowledge — I could not fetch the source pages
in this sandbox, so these are referenced citations to verify in PR review,
not freshly extracted figures):

- ClickHouse 24.10 GA announcement and `JSON` column docs describe per-path
  typed sub-columns with min/max stats and report 10–100× speedups vs.
  `JSONExtract(...)` over `String` storage, especially for `RANGE` and
  aggregation queries.
- Snowflake "Snowflake Elastic Data Warehouse" (Dageville et al., SIGMOD
  2016) §3.3.3 ("Semi-Structured Data") describes inferring "typed paths" at
  load time, materializing them as hidden typed columns, and pruning at
  query time using both the variant body and the shredded columns
  ("automatic type inference and columnar storage").
- The Dremel paper (Melnik et al., VLDB 2010) introduced repetition /
  definition levels — the canonical way to keep nested+repeated structure
  in flat columnar storage without combinatorial unnest. Parquet inherits
  this directly.
- Postgres `JSONB` + `GIN` answers containment / existence queries fast,
  but the planner cannot push aggregations into the index — `SUM(jb->>'x')`
  always materializes the row. This is the canonical "inverted-only" anti-
  pattern, and it's where Pinot is today.

Cite-in-PR (to be filled in by reviewer):
`https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse`,
ClickHouse docs `/sql-reference/data-types/json`,
Dageville et al. SIGMOD 2016 (Snowflake paper),
Melnik et al. VLDB 2010 (Dremel paper),
Postgres `jsonb` / `gin_jsonb_ops` documentation.

---

## 2. Storage format

### 2.1 Recommendation: separate shred files per path, registered as a new IndexType

Each shredded path is stored as a **standalone column-level index** on the
host column, with one file per shred. Reasons:

- It lets us reuse the existing single-column `ForwardIndexReader`,
  `Dictionary`, `RangeIndexReader`, `NullValueVectorReader` SPIs without
  inventing parallel readers inside the JSON index. The block-level
  min/max footer that `RangeIndexReader` and Pinot's standard fixed-width
  raw forward index already produce is exactly what we want for
  aggregation pushdown.
- It lets a partial reindex add or remove a single shred without
  rewriting the (potentially huge) `.json.idx` file.
- It keeps the inverted JSON index file format byte-for-byte unchanged for
  segments built without shreds, which is the rollback story (§ 4 and § 9).
- It mirrors how ClickHouse stores per-path "type sub-columns" as
  independent column files inside the same MergeTree part.

Concretely:

```
<segmentDir>/
  myCol.json.idx                       <-- unchanged inverted index
  myCol.json.shred.meta                <-- single small metadata file
  myCol.json.shred.{slug}.fwd          <-- raw forward index for each path
  myCol.json.shred.{slug}.nullvec      <-- nullability bitmap for each path
  myCol.json.shred.{slug}.range        <-- optional, range index
  myCol.json.shred.{slug}.dict         <-- optional, dictionary
```

where `{slug}` is `path-slug(jsonPath) + "." + dataType`. The slug must be
filesystem-safe (replace `$`, `.`, `[`, `]`, `*` with `_`) and the meta file
holds the authoritative path-string → slug mapping so the round-trip is
unambiguous. Including dataType in the slug means a path that is shredded
as both `STRING` and `INT` (rare, but allowed for path-type drift; see
§ 8) produces two distinct shred files.

`.json.shred.meta` (one tiny file per column):

```
version: int32                     // shred format version
numShreds: int32
for each shred:
  path: utf-8 string               // canonical "$.a.b" form
  dataType: enum byte              // FieldSpec.DataType ordinal
  slug: utf-8 string
  encoding: enum byte              // RAW | DICT | RLE | ...
  hasNullVec: bool
  hasRangeIndex: bool
  hasDictionary: bool
  source: enum byte                // USER_PINNED | AUTO_DISCOVERED
  numNonNullDocs: int64            // for cost-based routing
  flags: int32                     // reserved
```

The shred forward index itself is **a plain Pinot column forward index for
the host column's docId space** — same format the rest of the engine
already reads. No new reader. The host JSON column already has
`segmentMetadata.totalDocs` entries; the shred writes exactly that many
entries (per-doc, *not* per-flattened-record — see § 7 for nested-array
handling).

### 2.2 Why not extend `.json.idx`?

Tempting, but rejected:

- `.json.idx` is byte-laid-out as `[header][dictionary][inverted][docIdMapping]`
  with `HEADER_LENGTH = 32` and length-prefixed sections. Appending shreds
  requires a header version bump, and any rollback to an older binary
  fails to mmap the file. The version bump is unavoidable for typed
  shredding *eventually*, but bundling format change with feature rollout
  is a recipe for hard-to-debug mixed-version failures.
- The current `ImmutableJsonIndexReader` constructor reads four offsets at
  fixed positions. We would either grow the header (breaks old readers)
  or sentinel-pack (fragile).
- Per-path range/null/dictionary indexes correspond to existing per-column
  Pinot artifacts; co-locating them inside a JSON-index file requires
  parallel reader machinery we'd then have to maintain forever.

### 2.3 Dictionary sharing

For now, **each shred owns its dictionary** (or no dictionary, for raw fixed-
width types). Sharing a dictionary across shreds of the same column saves
space when many paths have overlapping value domains (e.g., country codes,
status enums) but introduces an N-way coupling at create time and a global
invalidation on a single path change. § 11 leaves this open.

### 2.4 Footer / block stats

The raw forward index Pinot already writes for fixed-width types embeds
sufficient block min/max for the `MIN`/`MAX`/`RANGE` fast path. For variable-
width STRING shreds we additionally write a segment-level min/max and
optionally per-1024-row block min/max into the `.range` file (existing
range-index format). No new format invented.

---

## 3. Schema discovery

Three options, with the trade-off cleanly stated:

| Option | Pro | Con |
|---|---|---|
| (a) User-configured only via `JsonIndexConfig.typedPaths` | Deterministic, debuggable, no surprise storage cost. | Users have to know their paths up-front. Misses opportunity on natural-growing schemas. |
| (b) Auto-discovery from first-N-row sampling | Zero-config win. | Sampling-window dependent — paths that appear late won't be shredded; type inference can be wrong on tail data; harder to reason about storage budget. |
| (c) Hybrid: user-pinned set + auto-promote at threshold | Best of both: predictable for known hot paths, opportunistic for new ones. | Twice the moving parts; needs commit-time decision logic that must agree across reload paths. |

**Recommendation: (a) for v1 (POC, this PR's scope), (c) for v2.**

Rationale: Pinot users today *already* tune `tableConfig`s by hand for
`inverted`, `range`, and `bloom` indexes; per-path shredding is a natural
extension of that vocabulary. Auto-discovery is a power feature but it
interacts with mixed-version segment compatibility (servers running an
older binary won't know what to do with auto-shredded segments) and with
storage budgeting. Ship (a), then add (c) gated by a config knob once we
have telemetry on per-path access frequency from real users.

ClickHouse's hybrid model is described in their 24.10 announcement: the
user can pin "type hints" per path, and dynamic paths still shred up to a
configurable limit (`max_dynamic_paths`) before falling into a "shared"
sub-column. F5 v2 should adopt this shape directly — pin set ∪ auto-set,
both capped, both materialized into the same shred file pool.

`JsonIndexConfig` gets one new field in v1 (POC):

```java
@JsonProperty("typedPaths")
private List<TypedPathSpec> _typedPaths;

public static class TypedPathSpec {
  private String _path;                  // canonical "$.a.b" — required
  private FieldSpec.DataType _dataType;  // required
  // v2: encoding hint, sample-decline reason, min/max overrides
}
```

A path missing from `typedPaths` continues to use the inverted index
exactly as today. A path present but absent from a given segment's data
materializes a fully-null shred (no docs match), so cross-segment readers
have a consistent layout.

---

## 4. Integration with the existing inverted index

**Recommendation: augment, not replace.** For each row, the existing
inverted index continues to record every (key, key+value) pair as it does
today. For each shredded path, in addition, we write the typed value into
the corresponding shred file.

Cost: ingest CPU and storage roughly double for the shredded paths. For a
table where ~10 of 200 paths are shredded, the marginal cost is small.
For tables that shred everything, this stops being free — that's an
acceptable forcing function for users to opt in.

Benefit: rollback story is trivial. If the shred reader code is buggy, the
planner falls back to today's `JsonMatchFilterOperator` /
`JsonExtractScalarTransformFunction` and gets correct (if slower) results.
A feature flag (`enableJsonIndexShredding=false`) turns off all routing
without touching segments on disk. Mixed-version clusters work: older
servers ignore the new shred files, newer servers see them.

Replacement (skip writing inverted entries for shredded paths) is rejected
for v1: it saves the marginal ingest cost but means a malformed predicate
or a new query pattern that the shred doesn't handle has to re-parse the
raw JSON. Worse, it means downgrading the server requires a segment
reload. v2 can revisit once we have real-world numbers.

---

## 5. Query routing

The planner needs four routing decisions, in order of POC scope:

### 5.1 Projection: `jsonExtractScalar(col, '<lit_path>', '<lit_type>')` (POC scope)

The transform-function factory (`TransformFunctionFactory`) is where we
intercept. When all of:

1. The first argument is an `IdentifierTransformFunction` resolving to a
   physical column whose JSON index config carries a `typedPaths` entry
   for the literal path,
2. The second argument is a `LiteralTransformFunction` (path string),
3. The third argument is a `LiteralTransformFunction` (type string),
4. The shred's dataType is "assignment-compatible" with the requested
   type (rules in § 5.4),

we substitute the standard `JsonExtractScalarTransformFunction` with a
new `ShreddedJsonExtractScalarTransformFunction` that holds a reference
to the shred's `ForwardIndexReader` and delegates `transformTo*ValuesSV`
straight to it. The fallback path (the existing transform function) is
chosen when any condition above fails.

### 5.2 Predicate pushdown: `JSON_MATCH(col, '<lit>')` (v2 scope, sketched here)

Extend `JsonMatchFilterOperator` (or insert a wrapper at
`FilterPlanNode.constructLeafFilterOperator` line ~332, the
`case JSON_MATCH` branch). On a single-clause predicate against a shredded
path, decompose to:

- Build a real `RangePredicate` / `EqPredicate` / `InPredicate` over a
  virtual column that points to the shred,
- Route through the existing per-column filter operator stack (which
  already knows how to use range indexes and forward indexes).

This lets `JSON_MATCH(col, '"$.ts" > 17e11')` route to
`RangeIndexFilterOperator`-equivalent, picking up min/max pruning.

### 5.3 Mixed predicate: `JSON_MATCH(col, '"$.x" > 10 AND "$.y" = ''a''')`

This is the critical case. Decomposition rule:

1. Parse the JSON-match filter to a `FilterContext` tree (already done
   inside `JsonMatchFilterOperator` today — `RequestContextUtils.getFilter
   (CalciteSqlParser.compileToExpression(filterString))`).
2. Walk the tree. For each leaf predicate, classify the LHS path:
   - **Shredded**: rewrite the predicate as a same-type predicate over a
     virtual column whose `ForwardIndexReader` is the shred. Add to the
     "shredded" sub-tree.
   - **Unshredded**: keep in the "inverted" sub-tree.
3. Combine via the original boolean structure. AND-of-(shredded ∧
   inverted) becomes `AND(shreddedFilter, jsonMatchFilter(inverted-only
   predicate))`. OR is similar but more care is needed because the JSON
   index's NOT/AND/OR semantics operate on flattened-doc space before
   the `getDocId` mapping. For unshredded paths we **must** preserve
   that semantic.

Special case: if any sub-predicate references an array path with `[*]`
under the same JSON column, the unflatten-then-AND semantic of today's
JSON index applies (a single root doc can have multiple flattened docs;
"NOT EQ" is computed *before* unflatten). Shredded paths don't have this
problem because they live in the unflattened doc-id space. Mixing them
under `AND` is fine. Under `OR` we have to be careful that the unflatten
domain is honored — concretely, if a non-shredded predicate excludes a
flattened doc via NOT_EQ, but a shredded predicate includes the root
doc, the union must use the root-doc-id space (which is what the shred
already lives in). The implementation rule: **always unflatten the
non-shredded sub-result first, then union/intersect.** This is exactly
what the current `getMatchingDocIds` flow does at the top level.

### 5.4 Type compatibility

A shred stored as `LONG` can serve a `jsonExtractScalar(..., 'INT')` only
if every value fits in `int` range; without per-segment min/max we can't
prove this cheaply. Conservative rule for POC:

- Routing fires only when requested type **exactly matches** shred dataType.
- v2: widen to "shred is at least as wide and signedness-compatible";
  fall back if the segment-level min/max footer says we'd overflow.

---

## 6. Mutable / consuming segments

**Recommendation: maintain shreds online for mutable segments.** ClickHouse
maintains the equivalent online (they call it the in-memory part). The
trade-offs:

| Approach | Pros | Cons |
|---|---|---|
| **Online** (maintain in `MutableJsonIndexImpl`) | Realtime queries get the speedup immediately. No commit-time latency tail. | Per-message CPU + memory overhead: every record adds N typed entries to N shred buffers in addition to the inverted entries it adds today. |
| **At commit only** | Zero per-message cost for shreds. | Commit takes longer; first immutable read-after-commit is fast, but the realtime window (potentially several minutes for Kafka, hours for Pulsar with low traffic) has no speedup. |

For F5, the right answer is **online for the shredded subset, commit-only
for everything else**. Justification:

- Pinot's Kafka/Pulsar consumers already do per-message work proportional to
  the number of indexes (`forward + inverted + range + bloom + json + ...`).
  Adding `O(numShreds)` more is a constant additive cost; it's the same
  asymptotic budget. The user opted in by configuring typed paths.
- The hot-path cost is bounded: parsing already happened (the
  `MutableJsonIndexImpl.add(String)` path does `JsonUtils.flatten` which
  has already walked the tree). Extracting typed values for the shredded
  paths reuses that walk — we don't re-parse.
- Realtime-segment latency tail at commit (option (b)) is much more
  observable as a user-facing SLO than +N% CPU during steady-state ingest.

Concretely: extend `MutableJsonIndexImpl` to take a `List<TypedPathSpec>`
and, during `addFlattenedRecords` or earlier (during the same walk that
produces flattened records), pop typed values into per-shred arrays
(`IntArrayList`, `LongArrayList`, `String[]`, etc.). At commit, flush
each shred to its forward-index file in the same `convertAndSeal`
ordering pass used for the existing inverted JSON index.

Memory accounting: extend the existing `MUTABLE_JSON_INDEX_MEMORY_USAGE`
metric to include shred bytes; respect `maxBytesSize`.

---

## 7. Nested arrays and the unnest problem

Today's `JsonUtils.flatten(...)` (pinot-spi `JsonUtils.java` ~line 420)
does Cartesian unnest. For a doc with 3 addresses and 4 skills, the
flattened result is 12 rows under default config. With
`disableCrossArrayUnnest=true`, it becomes 3+4=7. With a 100k cap, large
JSON docs with multiple sibling arrays just refuse to index.

### 7.1 Repetition / definition levels

Dremel's solution, encoded in Parquet, is **repetition + definition
levels** per leaf value:

- The **definition level** is the depth at which the leaf's value (or
  null) was actually present. Lower = nullness deeper in the path.
- The **repetition level** is the depth at which the value's *array
  parent* starts repeating. Zero means "new top-level record".

The pair (rep, def) per value, combined with the schema, exactly
reconstructs the original document. Crucially, the per-value storage is
**O(actual data)**, not O(cross product) — no combinatorial blowup.

### 7.2 Mapping to Pinot

Add two parallel `byte`-or-`short`-sized streams per shred:

```
<segmentDir>/myCol.json.shred.addresses_street_STRING.fwd       # values
<segmentDir>/myCol.json.shred.addresses_street_STRING.replvl    # rep levels
<segmentDir>/myCol.json.shred.addresses_street_STRING.deflvl    # def levels
```

The rep/def streams are the same length as `.fwd`, and use Pinot's existing
fixed-width MV-forward-index format (each value sits at a known byte offset).
Both streams are tiny in practice — for typical depths (≤8) one byte each is
plenty, so they're effectively a 2-byte-per-value tax.

For the projection slice the POC implements, MV results are flattened back to
the host-doc id space at read time using the rep stream (it tells us where one
doc's values end and the next begins). For SV paths, rep/def streams degrade
into a flat array — the POC starts there and lifts to the full nested form in
v2.

This solves the cross-array explosion. The 100k cap can be relaxed for
shredded paths; non-shredded paths keep today's behavior.

---

## 8. Schema evolution

| Event | Behavior at query time |
|---|---|
| **New path appears mid-table.** Older segments don't have a shred for it; newer segments do. | Per-segment routing: the planner inspects `segmentMetadata` on the JSON index column for each segment. If the segment is "old" (no shred for the path), it routes the projection / predicate to the legacy `JsonExtractScalarTransformFunction` / `JsonMatchFilterOperator`. Mixed-segment plans return correct (uneven-performance) results. |
| **Previously sparse path becomes dense.** | Two sub-cases. (a) Pinned shred from day one: no change, the shred is already there, it was just mostly nulls. (b) Auto-promoted (v2): a background re-index task materializes the shred for the segments that don't yet have it. While it's running, queries continue to hit the legacy path for those segments. |
| **Path changes type (e.g., `"42"` → `42`).** | If the user updates `typedPaths` for the column to the new type, only newly-built segments get the new-type shred. Existing segments keep the old-type shred. The planner's type-compatibility check (§ 5.4) refuses to route projections / predicates with the wrong requested type — they fall back to the legacy path, which already does string-or-numeric coercion. v2: support multi-type shreds for a single path (a `STRING` and an `INT` shred coexist, and the planner picks based on requested type) — analogous to ClickHouse's "variant of variants". |

In all three cases the rule is the same: **the shred is a fast-path
opt-in, never load-bearing for correctness.** If routing isn't possible,
the legacy code path runs. This is what makes augmentation (§ 4) load-
bearing.

---

## 9. Migration & rollout

Recommended sequence:

1. **Land the shred file format, off by default.** Ship the new
   `typedPaths` config, the creation hook, and the reader. No segments
   are affected until a user opts in. Mixed-version clusters with
   different binaries see no change unless `typedPaths` is set.
2. **Per-table opt-in.** Users add `typedPaths` to the table config, and
   only new segments (from realtime commit, batch ingest, or explicit
   reindex) materialize shreds.
3. **Lazy materialization at next reload.** When a segment is reloaded
   (via `SegmentPreProcessor` running a `JsonIndexHandler`-equivalent
   `JsonShredIndexHandler`), missing shreds are built from the live JSON
   column. This reuses the same handler shape that today rebuilds JSON
   indexes when a config changes; we just attach a new handler.
4. **Background re-index (later).** Add a minion task
   `JsonShredRefreshTask` that walks segments and triggers reloads with
   missing shreds. Optional; users who can tolerate next-reload-window
   lag don't need it.

Rollback at every step is rollback to the legacy reader. The shred files
on disk are *cheap to delete* and *safe to leave* — older binaries
ignore them.

For mixed-version brokers / servers: the broker doesn't care about
shreds, only the server execution layer does. A broker running an older
binary plans queries the same way; the server that sees a typed shred
file may or may not use it. Servers that don't recognize the file simply
ignore it and fall back. No protocol changes.

---

## 10. Performance model

Per-predicate / per-op, qualitative expectations (these inform the
benchmark plan, not promises):

| Op | Today | With shred | Why |
|---|---|---|---|
| `EQ` on numeric path | dict lookup on stringified value | dict lookup on typed value (or range-index point lookup) | Removes string parse. Same posting-list cost. Modest (2–3×). |
| `EQ` on STRING path | dict lookup | identical or marginally worse (extra file open) | No fundamental gain; not the target case. |
| `IN` (N values) | N dict lookups | N typed lookups, or one range scan if values are dense | Comparable; marginal win at large N. |
| `RANGE` on numeric path | full dict scan + per-value DOUBLE parse + OR-merge | block-min/max-pruned scan of typed forward index | 10–100×: this is the headline win. Matches ClickHouse's reported numbers. |
| `REGEXP_LIKE` on string path | full dict scan with Java regex | unchanged, regex still needs to evaluate every value | Roughly break-even. If we add an FST index per STRING shred (v3), big win. |
| `MIN` / `MAX` on numeric path | not pushed down — `JsonExtractScalar` evaluated per row | answered from per-segment min/max footer | ~100× (segment-level), can be ~1000× if min/max alone resolves. |
| `SUM` / `AVG` on numeric path | per-row Jackson parse | direct read from packed-doubles forward index | 30–50× from removing parse overhead alone. |
| `GROUP BY + COUNT(*)` on a typed-path key | per-row Jackson parse to get group key | direct dictionary-id read | 20–30×. |
| `GROUP BY + SUM(other typed-path)` | two per-row Jackson parses | two forward-index reads | 30–50× combined. |
| `IS NULL` / `IS NOT NULL` on shredded path | dict lookup on the bare key | direct null-bitmap read | 5–10× — small absolute win, but the access pattern is much friendlier to vectorization. |

These are the model. The bench-compare skill on this branch's POC will
fill in the actual numbers for the projection slice — predicate pushdown
won't land until v2.

---

## 11. Open questions

These were the points I deliberately did not commit to in v1 and want
human eyes on:

1. **Dictionary sharing across shreds of the same column.** Saves space
   for status-enum-shaped paths (`country`, `region`, `status`). Costs:
   shared write-side state at create time; one-path edits invalidate
   shared dict ids. Lean toward "no" for v1, but worth a per-column
   opt-in flag eventually. Decision deferred.
2. **Storage budget heuristic.** At what point does adding the N+1st shred
   stop being a win? Should we surface a "this path was sampled, type
   inferred as X, but not shredded because budget" annotation back to the
   user? Likely yes — we'll add a `controller`-side admin endpoint that
   reports per-segment shred coverage.
3. **Variant-of-variants (mixed-type paths).** ClickHouse handles `{"x":
   "a"}` and `{"x": 42}` in the same column by giving `$.x` both a
   `String` and an `Int64` shred, plus a discriminator. F5 v1 takes the
   simpler stance: one shred per (path, dataType). If a value doesn't fit
   the declared type, it goes to null and the original is still in the
   inverted index. Should v2 model multi-type explicitly?
4. **REST/JSON serialization of `TypedPathSpec` and rolling-upgrade
   safety.** Adding a new optional field to `JsonIndexConfig` is the
   simplest path and is what the POC does. Confirm with a config-back-
   compat reviewer that nothing breaks for clusters with a mix of older
   controllers.
5. **Interaction with composite JSON index** (`composite_json_index`,
   referenced in `FilterPlanNode.constructLeafFilterOperator`). That
   plugin already implements path-selective indexing. Is typed shredding
   layered on top, or is it a sibling? My assumption: sibling — composite
   JSON has its own SPI shape, and typed shredding is a property of the
   standard JSON index. Worth confirming with whoever owns composite.

---

## Appendix: POC scope (this branch)

This branch lands the minimum viable slice that proves the routing
skeleton:

1. `JsonIndexConfig.TypedPathSpec` data class + `typedPaths` field, JSON-
   serializable.
2. A `JsonIndexShredder` that, given a `TypedPathSpec` list, extracts the
   typed value per doc and writes it to a sidecar file
   (`<col>.<slug>.shred.fwd`). For the POC this is a STRING-only
   length-prefixed file — we deliberately don't reuse the production
   forward-index writer to keep the POC small, and the production-grade
   format choice is itemized in § 2 above as future work.
3. A `JsonIndexShredReader` that mmaps the sidecar.
4. A `ShreddedJsonExtractScalarTransformFunction` that, when constructed,
   detects a routable shape and reads from the shred instead of parsing
   the JSON.
5. One TestNG test that builds a tiny segment with a STRING shred,
   queries `jsonExtractScalar(col, '$.x', 'STRING')`, and asserts
   parity with the unshredded baseline.

Things deferred:
- Range index on shreds.
- Predicate pushdown (`JSON_MATCH` → shred).
- Auto-discovery.
- Repetition / definition levels (POC handles SV paths only).
- Migration from existing segments.
- Mutable / consuming segment support.
- Dictionary sharing.
- Performance benchmarks. The infra (`bench-compare` skill) is in place
  for v2 to use.
