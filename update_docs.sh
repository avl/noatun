# Script to convert `docs.md` to `docs-svg.md`.
# This replaces all the mermaid diagrams in `docs.md` with svg
cd doc_helper && cargo run && cd .. && cargo doc
