# company_intelligence.py
# Company Intelligence – CSV cleaner/deduper (single-file version)

import os
import re
import argparse
import pandas as pd

# ---------- Cleaning ----------

_SUFFIXES = [
    r"\bLTD\b",
    r"\bLIMITED\b",
    r"\bLLP\b",
    r"\bPLC\b",
    r"\bINC\b",
    r"\bCORP\b",
    r"\bL\.?L\.?P\.?(?:\b|$)",
    r"\bL\.?T\.?D\.?(?:\b|$)",
    r"\bP\.?L\.?C\.?(?:\b|$)",
]
_SUFFIX_PATTERNS = [re.compile(p) for p in _SUFFIXES]

def normalise_company_name(name: str, strip_suffixes: bool = True) -> str:
    """Deterministically normalise a company name."""
    if not isinstance(name, str):
        return ""
    s = name.strip().replace("’", "'").replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s).upper()
    # unwrap parentheses but keep content: "ACME (HOLDINGS) PLC" -> "ACME HOLDINGS PLC"
    s = re.sub(r"\s*\(([^)]+)\)\s*", r" \1 ", s)
    if strip_suffixes:
        pad = f" {s} "
        for pat in _SUFFIX_PATTERNS:
            pad = pat.sub(" ", pad)
        s = pad.strip()
    # don't strip parentheses now (already unwrapped); trim other punctuation
    s = s.strip(" .,-_[]{}")
    s = re.sub(r"\s+", " ", s)
    return s

# ---------- Dedupe & I/O ----------

def add_normalised_column(df: pd.DataFrame, col: str = "company_name", strip_suffixes: bool = True) -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")
    out = df.copy()
    out["_norm_name"] = out[col].astype(str).map(lambda x: normalise_company_name(x, strip_suffixes=strip_suffixes))
    return out

def dedupe_by_normalised_name(df: pd.DataFrame) -> pd.DataFrame:
    if "_norm_name" not in df.columns:
        df = add_normalised_column(df)
    return df.drop_duplicates(subset=["_norm_name"]).reset_index(drop=True)

def _ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

def read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8", dtype=str)

def write_outputs(df: pd.DataFrame, out_csv: str) -> None:
    _ensure_parent(out_csv)
    df.to_csv(out_csv, index=False, encoding="utf-8")
    try:
        df.to_excel(out_csv.replace(".csv", ".xlsx"), index=False)
    except Exception:
        pass

# ---------- Orchestration ----------

def run(in_path: str, out_path: str, keep_suffixes: bool = False, keep_norm_col: bool = False, verbose: bool = False) -> None:
    if not os.path.exists(in_path):
        raise FileNotFoundError(
            f"Input not found: {in_path}\n"
            f"Working dir: {os.getcwd()}\n"
            "Tip: put your CSV under a 'Data' folder next to this script, or pass an absolute path."
        )

    df = read_csv(in_path)
    n_in = len(df)

    df = add_normalised_column(df, strip_suffixes=not keep_suffixes)
    df = dedupe_by_normalised_name(df)
    n_out = len(df)

    if not keep_norm_col and "_norm_name" in df.columns:
        df = df.drop(columns=["_norm_name"])

    write_outputs(df, out_path)

    if verbose:
        print(f"Read {n_in} rows → {n_out} unique (removed {n_in - n_out}).")
        print(f"Wrote: {out_path} and {out_path.replace('.csv', '.xlsx')}")

# ---------- CLI ----------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Company Intelligence – CSV cleaner/deduper")
    parser.add_argument("--in", dest="in_path", required=True, help="Input CSV with at least 'company_name'")
    parser.add_argument("--out", dest="out_path", required=True, help="Output CSV path (CSV + Excel are written)")
    parser.add_argument("--keep-suffixes", action="store_true",
                        help="Don't strip legal suffixes (Ltd/LLP/PLC) when normalising")
    parser.add_argument("--keep-norm-col", action="store_true",
                        help="Keep the internal _norm_name column in the output")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print a summary after run")
    args = parser.parse_args()
    run(args.in_path, args.out_path,
        keep_suffixes=args.keep_suffixes,
        keep_norm_col=args.keep_norm_col,
        verbose=args.verbose)
