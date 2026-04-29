"""Ingestion layer: source file -> raw_eudragmdp (SQLite, append-only)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "qualifyze.db"

# Canonical column rename map for the EudraGMDP export (the only source we
# currently support). Future sources would have their own rename map and
# would be branched on `source`.
EUDRAGMDP_COLUMN_RENAME = {
    "Certificate Number": "certificate_number",
    "EudraGMDP Document Reference Number": "eudragmdp_doc_ref",
    "Document Type": "document_type",
    "MIA Number": "mia_number",
    "OMS Organisation Identifier": "oms_organisation_id",
    "OMS Location Identifier": "oms_location_id",
    "Site Name": "site_name",
    "Address 1": "address_1",
    "Address 2": "address_2",
    "Address 3": "address_3",
    "Address 4": "address_4",
    "City": "city",
    "Postcode": "postcode",
    "Country": "country",
    "DUNS Number": "duns_number",
    "Site NCA Reference": "site_nca_reference",
    "Inspection End Date": "inspection_end_date",
    "Issue Date": "issue_date",
    "Last Updated Date": "last_updated_date",
}

REQUIRED_HEADER_MARKER = "EudraGMDP Document Reference Number"


def fetch_source(url: str):
    """Stub. In production this would fetch the source export from URL."""
    print(f"In production this would fetch from URL: {url}")
    return None


def _pick_engine(path: Path) -> str:
    """xlrd for true .xls (BIFF), openpyxl if it's actually .xlsx."""
    with open(path, "rb") as f:
        magic = f.read(8)
    if magic[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "xlrd"
    if magic[:2] == b"PK":
        return "openpyxl"
    return "xlrd"


def _detect_header_row(path: Path, engine: str) -> int:
    """Look for the marker column in the first 20 rows.

    Case- and whitespace-insensitive: 'EudraGMDP Document Reference Number',
    'eudragmdp document reference number' and ' EUDRAGMDP DOCUMENT REFERENCE
    NUMBER ' are all considered a match.
    """
    probe = pd.read_excel(path, engine=engine, header=None, nrows=20)
    marker = REQUIRED_HEADER_MARKER.strip().lower()
    for idx, row in probe.iterrows():
        cells = [str(c).strip().lower() for c in row.values]
        if marker in cells:
            return idx
    raise ValueError(
        f"Could not find header row (looked for '{REQUIRED_HEADER_MARKER}' "
        f"case-insensitively in the first 20 rows of {path.name})."
    )


def _read_eudragmdp_excel(path: Path) -> pd.DataFrame:
    engine = _pick_engine(path)
    header_row = _detect_header_row(path, engine)
    print(f"[ingest] engine={engine}, header_row={header_row}")
    df = pd.read_excel(path, engine=engine, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # Case-insensitive match between expected headers and what's in the file.
    # We index the actual headers by their lowercase form, then look up each
    # expected name. This means 'Country', 'COUNTRY' and 'country' all resolve
    # to the same canonical column.
    by_lower = {c.lower(): c for c in df.columns}
    rename_map: dict[str, str] = {}
    missing = []
    for expected, canonical in EUDRAGMDP_COLUMN_RENAME.items():
        actual = by_lower.get(expected.lower())
        if actual is None:
            missing.append(expected)
        else:
            rename_map[actual] = canonical

    if missing:
        raise ValueError(
            f"Excel is missing expected columns: {missing}\n"
            f"Columns actually found: {list(df.columns)}"
        )

    # Warn (don't fail) on columns we did not expect.
    canonical_set = set(EUDRAGMDP_COLUMN_RENAME.values())
    extras = [c for c in df.columns if c not in rename_map]
    if extras:
        print(f"[ingest] WARNING: dropping {len(extras)} unexpected column(s) "
              f"that are not part of the canonical schema: {extras}")

    df = df.rename(columns=rename_map)
    df = df[list(canonical_set & set(df.columns))]
    df = df[list(EUDRAGMDP_COLUMN_RENAME.values())]
    for col in df.columns:
        df[col] = df[col].astype("object")
    df = df.where(pd.notnull(df), None)
    return df


def _ensure_source_column(conn: sqlite3.Connection) -> None:
    """If raw_eudragmdp exists without a `source` column (older runs), add it."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_eudragmdp'"
    )
    if cur.fetchone() is None:
        return  # table doesn't exist yet; pandas to_sql will create it
    cols = {row[1] for row in conn.execute("PRAGMA table_info(raw_eudragmdp)")}
    if "source" not in cols:
        print("[ingest] Adding missing `source` column to existing raw_eudragmdp")
        conn.execute("ALTER TABLE raw_eudragmdp ADD COLUMN source TEXT")
        conn.commit()


def ingest(batch_id: str, source: str, file_path: str) -> int:
    """Read a source export file and append rows into raw_eudragmdp.

    Args:
        batch_id:  pipeline run identifier (e.g. 'run_20260427_204811').
        source:    logical name of the upstream feed, e.g. 'eudragmdp'.
        file_path: absolute or relative path to the export file.

    Returns the number of rows ingested.
    """
    src_path = Path(file_path).expanduser().resolve()
    if not src_path.exists():
        raise FileNotFoundError(f"Source file not found at {src_path}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"[ingest] source={source}, file={src_path}")

    if source == "eudragmdp":
        df = _read_eudragmdp_excel(src_path)
    else:
        raise NotImplementedError(
            f"No reader configured for source='{source}'. "
            f"Add a branch in ingest.py."
        )

    df["ingestion_batch_id"] = batch_id
    df["source"] = source

    n = len(df)
    print(f"[ingest] {n} rows read; appending to raw_eudragmdp at {DB_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        _ensure_source_column(conn)
        df.to_sql("raw_eudragmdp", conn, if_exists="append", index=False)

    print(f"[ingest] Appended {n} rows with batch_id={batch_id}, source={source}")
    return n


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--file", required=True)
    args = p.parse_args()
    ingest(
        batch_id="run_" + datetime.now().strftime("%Y%m%d_%H%M%S"),
        source=args.source,
        file_path=args.file,
    )
