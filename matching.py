"""Match silver_eudragmdp_sites to gold_master_site (Qualifyze internal registry).

Pass 1 - exact match on oms_location_id  -> match_confidence='exact'
Pass 2 - fuzzy match on (site_name, country) using rapidfuzz, threshold=85
                                            -> match_confidence='fuzzy'
Otherwise                                  -> match_confidence='unmatched', site id NULL
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "qualifyze.db"
FUZZY_THRESHOLD = 85


def run_matching(db_path: Path | None = None) -> dict:
    db = Path(db_path) if db_path else DB_PATH
    print(f"[matching] Using database {db}")

    with sqlite3.connect(db) as conn:
        sites = pd.read_sql_query("SELECT * FROM silver_eudragmdp_sites", conn)
        master = pd.read_sql_query("SELECT * FROM gold_master_site", conn)

    if sites.empty:
        print("[matching] silver_eudragmdp_sites is empty - nothing to match")
        return {"exact": 0, "fuzzy": 0, "unmatched": 0}

    # Initialise.
    sites["qualifyze_site_id"] = None
    sites["match_confidence"] = "unmatched"

    # ---- Pass 1: exact match on oms_location_id ------------------------------
    master_by_oms = (
        master.dropna(subset=["oms_location_id"])
        .drop_duplicates(subset=["oms_location_id"])
        .set_index("oms_location_id")["qualifyze_site_id"]
        .to_dict()
    )
    exact_hits = 0
    for idx, row in sites.iterrows():
        oms = row.get("oms_location_id")
        if oms is not None and oms in master_by_oms:
            sites.at[idx, "qualifyze_site_id"] = master_by_oms[oms]
            sites.at[idx, "match_confidence"] = "exact"
            exact_hits += 1
    print(f"[matching] Pass 1 (exact on oms_location_id): {exact_hits} matches")

    # ---- Pass 2: fuzzy match on site_name + country --------------------------
    fuzzy_hits = 0
    # Build per-country lookup of master rows for efficiency.
    master_by_country: dict[str, pd.DataFrame] = {
        c: g for c, g in master.groupby("country", dropna=False)
    }

    unmatched_mask = sites["match_confidence"] == "unmatched"
    for idx, row in sites[unmatched_mask].iterrows():
        country = row.get("country")
        name = row.get("site_name")
        if not country or not name or country not in master_by_country:
            continue
        candidates = master_by_country[country]
        choices = candidates["canonical_name"].tolist()
        if not choices:
            continue
        match = process.extractOne(
            str(name), choices, scorer=fuzz.WRatio, score_cutoff=FUZZY_THRESHOLD
        )
        if match is None:
            continue
        matched_name, score, pos = match
        site_id = candidates.iloc[pos]["qualifyze_site_id"]
        sites.at[idx, "qualifyze_site_id"] = site_id
        sites.at[idx, "match_confidence"] = "fuzzy"
        fuzzy_hits += 1
    print(f"[matching] Pass 2 (fuzzy on site_name+country): {fuzzy_hits} matches")

    unmatched = int((sites["match_confidence"] == "unmatched").sum())
    print(f"[matching] Unmatched: {unmatched}")

    # Persist updates back to silver_eudragmdp_sites.
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE silver_eudragmdp_sites "
            "SET qualifyze_site_id = NULL, match_confidence = 'unmatched'"
        )
        for _, row in sites.iterrows():
            cur.execute(
                "UPDATE silver_eudragmdp_sites "
                "SET qualifyze_site_id = ?, match_confidence = ? "
                "WHERE oms_location_id = ?",
                (
                    row["qualifyze_site_id"],
                    row["match_confidence"],
                    row["oms_location_id"],
                ),
            )
        conn.commit()

    return {"exact": exact_hits, "fuzzy": fuzzy_hits, "unmatched": unmatched}


if __name__ == "__main__":
    run_matching()
