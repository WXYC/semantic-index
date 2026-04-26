# M0.2 — semantic-index mojibake duplicate-artist audit

Total pairs: 0
Total edges affected: 0
Total plays affected: 0

## Scan diagnostics

- Total artists scanned: 136702
- Round-trippable mojibake names (latin1->utf8 reversible): 0
- Lossy-mojibake names (contain `?` + latin1-supplement chars, unrecoverable here): 73

A pair is reported only when **both** the corrupted form and its round-trippable fixed form exist as separate artist rows. Lossy-mojibake names cannot be auto-recovered and require V013's human-reviewed lossy mappings to detect any corresponding duplicates. M2.2 will need to re-scan after V012 propagates and V013 lands.

## Top 0 pairs by combined edge count

_No round-trippable duplicate pairs found._
