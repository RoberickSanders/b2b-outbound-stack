# stubs/

Stand-ins for modules that stay private in the production deployment
(vendor request code for discovery, enrichment, scoring and export, plus
tuned per-client logic).

They keep the import surface identical so that:

- every module in the repo imports in a fresh venv,
- `python doctor.py --fast` and `python forge.py "<brief>" --dry-run` run,
- the pytest suite runs offline with no API keys.

Calling any function in a stand-in raises `PrivateModuleNotIncluded` with
the module and function name. Nothing fails silently.

To run the full pipeline, add your own module with the same name in the
repo root or `tools/`. `stubs/` is appended to the end of `sys.path`, so a
real module always wins.

| Stand-in | What the private module does |
|---|---|
| `v2_discovery` | Company discovery (Blitz, Serper) |
| `v2_aiark` | Lookalike discovery and people search (AI Ark) |
| `v2_enrichment` | Blitz contact enrichment + backfill |
| `v2_scoring` | Tiering + bulk email verification |
| `v2_cleaning` | Row cleaning before/after enrichment |
| `v2_export` | Dedup + CSV export |
| `v2_config`, `v2_cache`, `v2_checkpoint` | Run config, cache, resumable checkpoints |
| `icp`, `scraping`, `signals`, `title_enrichment`, `name_discovery` | Legacy local-business pipeline (`lead_pipeline_v6.py`) |
