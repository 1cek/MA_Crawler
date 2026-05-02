#!/usr/bin/env python3
"""
Dedupliziert eine JSONL-Datei nach folgenden Regeln (in dieser Reihenfolge):

1. Gleiche URL + gleicher Body → nur den frühesten Snapshot behalten.
2. Gleiche ID, unterschiedlicher Body → ID umbenennen (suffix _2, _3, ...).
3. Gleicher Body, verschiedene URLs → nur den "ersten" behalten:
   erstens nach frühestem `date`, dann nach frühestem `snapshot_timestamp`,
   dann nach Position in der Liste.

Nicht berührt:
- Gleiche URL, unterschiedlicher Body → alle behalten (echte Versionen).

Benutzung:
  python3 deduplicate_jsonl.py input.jsonl
  python3 deduplicate_jsonl.py input.jsonl -o output.jsonl
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict


def load_records(path: Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  ⚠️  Zeile {i} konnte nicht geparst werden: {e}")
    return records


def snapshot_sort_key(r: dict) -> tuple:
    """Sortierschlüssel: frühestes date, dann frühester snapshot_timestamp."""
    return (r.get("date") or "", r.get("snapshot_timestamp") or "")


# ── Regel 1: Gleiche URL + gleicher Body → frühesten Snapshot behalten ────────

def rule1_same_url_same_body(records: list[dict]) -> tuple[list[dict], list[dict]]:
    # Bestimme pro (url, body) den frühesten Eintrag
    winner: dict[tuple, dict] = {}
    for r in records:
        key = (r.get("url", ""), r.get("body", ""))
        if key not in winner or snapshot_sort_key(r) < snapshot_sort_key(winner[key]):
            winner[key] = r

    kept, removed = [], []
    seen: set[tuple] = set()
    for r in records:
        key = (r.get("url", ""), r.get("body", ""))
        w = winner[key]
        if key not in seen and r.get("snapshot_timestamp") == w.get("snapshot_timestamp"):
            seen.add(key)
            kept.append(r)
        else:
            removed.append(r)
            print(f"  [Regel 1] Entfernt: snap={r.get('snapshot_timestamp')}  id={r.get('id')}  url={r.get('url','')[:70]}")

    return kept, removed


# ── Regel 2: Gleiche ID, unterschiedlicher Body → ID umbenennen ───────────────

def rule2_rename_duplicate_ids(records: list[dict]) -> list[dict]:
    id_counter: dict[str, int] = {}
    result = []
    for r in records:
        original_id = r.get("id", "")
        if original_id not in id_counter:
            id_counter[original_id] = 1
            result.append(r)
        else:
            id_counter[original_id] += 1
            new_id = f"{original_id}_{id_counter[original_id]}"
            r = dict(r)
            r["id"] = new_id
            print(f"  [Regel 2] ID umbenannt: {original_id} → {new_id}  (snap={r.get('snapshot_timestamp')})")
            result.append(r)
    return result


# ── Regel 3: Gleicher Body, verschiedene URLs → erste URL behalten ────────────

def rule3_same_body_different_url(records: list[dict]) -> tuple[list[dict], list[dict]]:
    # Bestimme pro Body den "frühesten" Eintrag
    winner: dict[str, dict] = {}
    for r in records:
        body = r.get("body", "")
        if body not in winner or snapshot_sort_key(r) < snapshot_sort_key(winner[body]):
            winner[body] = r

    kept, removed = [], []
    seen_bodies: set[str] = set()
    for r in records:
        body = r.get("body", "")
        w = winner[body]
        is_winner = (
            r.get("url") == w.get("url") and
            r.get("snapshot_timestamp") == w.get("snapshot_timestamp")
        )
        if body not in seen_bodies and is_winner:
            seen_bodies.add(body)
            kept.append(r)
        elif body in seen_bodies:
            removed.append(r)
            print(f"  [Regel 3] Entfernt (gleicher Body): id={r.get('id')}  url={r.get('url','')[:70]}")
        else:
            # Body noch nicht gesehen, aber nicht der Winner-Snapshot → trotzdem behalten
            # (passiert wenn Winner bereits durch Regel 1 entfernt wurde – sicherheitshalber)
            seen_bodies.add(body)
            kept.append(r)

    return kept, removed


def write_jsonl(path: Path, records: list[dict]):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Dedupliziert eine JSONL-Datei.")
    parser.add_argument("input", type=Path, help="Eingabe-JSONL-Datei")
    parser.add_argument("-o", "--output", type=Path, default=None,
                        help="Ausgabe-JSONL (Standard: <input>_dedup.jsonl)")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"❌ Datei nicht gefunden: {args.input}")
        return

    output = args.output or args.input.with_stem(args.input.stem + "_dedup")

    print(f"\n📂 Lese: {args.input}")
    records = load_records(args.input)
    print(f"   {len(records)} Einträge eingelesen\n")

    print("── Regel 1: Gleiche URL + gleicher Body ──────────────────────────────")
    records, removed1 = rule1_same_url_same_body(records)
    if not removed1:
        print("   (keine Duplikate gefunden)")

    print("\n── Regel 2: Gleiche ID, unterschiedlicher Body → umbenennen ──────────")
    records = rule2_rename_duplicate_ids(records)

    print("\n── Regel 3: Gleicher Body, verschiedene URLs ──────────────────────────")
    records, removed3 = rule3_same_body_different_url(records)
    if not removed3:
        print("   (keine Duplikate gefunden)")

    # Abschluss-Check: sind alle IDs jetzt unique?
    ids = [r.get("id") for r in records]
    dupes = [id_ for id_ in ids if ids.count(id_) > 1]
    if dupes:
        print(f"\n  ⚠️  Noch doppelte IDs nach allen Regeln: {set(dupes)}")

    print(f"\n── Ergebnis ───────────────────────────────────────────────────────────")
    print(f"   Entfernt (Regel 1): {len(removed1)}")
    print(f"   Entfernt (Regel 3): {len(removed3)}")
    print(f"   Gesamt entfernt:    {len(removed1) + len(removed3)}")
    print(f"   Verbleibend:        {len(records)}")

    write_jsonl(output, records)
    print(f"\n✅ Gespeichert: {output}\n")


if __name__ == "__main__":
    main()
    print("done")