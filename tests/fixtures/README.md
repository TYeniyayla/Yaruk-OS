# Golden dataset (fixtures)

This folder holds **reference PDFs** and, where available, **expected outputs** for regression testing (layout fidelity, reading order, tables, formulas).

## Planned categories

1. **`academic/`** — Multi-column, equation-heavy papers  
2. **`datasheet/`** — Dense tables and schematics  
3. **`scanned/`** — Scan/OCR-heavy documents  
4. **`multicolumn/`** — Complex column layouts  
5. **`table_heavy/`** — Table-dominant pages  

## Per-category layout (target)

```
<category>/
  input.pdf
  expected_merged.md
  expected_merged.json
  thresholds.json
```

## Metrics (targets)

- Layout fidelity (structure vs reference)  
- Reading-order accuracy  
- Table structure score  
- Formula / LaTeX consistency  
