#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
04T_validacion_v6.py
====================
Fixes vs v5:
  1. Fuerza engine="xlrd" para .xls — openpyxl falla con "File is not a zip file"
     para archivos .xls (formato Excel 97-2003).
  2. COL_TOTAL ya no es fijo (=1). Después de encontrar el label en columna j,
     busca el primer valor numérico a la derecha de j.
     Esto es robusto ante columnas vacías/combinadas al inicio de la hoja.
  3. Prueba de lectura al inicio (1 archivo) para confirmar que xlrd funciona.
"""

import re
import sys
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# ============================================================
# CONFIG
# ============================================================
BASE_DIR_MANUAL = None
# BASE_DIR_MANUAL = Path(r"C:\Users\vicou\...\ENOE")

UMBRAL_PP = 0.5

LABELS_P15   = ["poblacion de 15","15 anos y mas","2. poblaci"]
LABELS_PEA   = ["economicamente activa (pea)","pea)","poblacion economicamente activa"]
LABELS_DESOC = ["desocupada","desocupado"]
LABELS_TPART = ["tasa de participaci","t. de participaci","tasas de participaci"]
LABELS_TDESOC= ["tasa de desocupaci","t. de desocupaci","tasas de desocupaci"]


# ============================================================
# HELPERS
# ============================================================
def _norm(x) -> str:
    if pd.isna(x): return ""
    s = str(x).strip().lower()
    for a, b in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ü","u"),("ñ","n")]:
        s = s.replace(a, b)
    return s

def _as_float(x):
    if pd.isna(x): return None
    s = str(x).strip().replace(",","").replace("%","")
    try:
        v = float(s)
        return None if v != v else v   # descarta NaN
    except: return None


def find_label_get_total(df: pd.DataFrame, labels: list):
    """
    Busca cualquiera de los labels en las primeras 8 columnas de cada fila.
    Cuando lo encuentra en columna j, devuelve el PRIMER valor numérico
    a la derecha de j (no una columna fija).
    Esto es robusto ante celdas combinadas/vacías antes del label.
    """
    labels_norm = [_norm(l) for l in labels]

    for i in range(len(df)):
        for j in range(min(df.shape[1], 8)):
            cell = _norm(df.iat[i, j])
            if any(lbl in cell for lbl in labels_norm):
                # Busca el primer número a la derecha del label
                for k in range(j + 1, min(df.shape[1], j + 12)):
                    v = _as_float(df.iat[i, k])
                    if v is not None:
                        return v
    return None


# ============================================================
# EXCEL — fuerza xlrd para .xls
# ============================================================
def require_xlrd():
    """Verifica que xlrd esté instalado. Sale con mensaje claro si no."""
    try:
        import xlrd  # noqa
        return True
    except ImportError:
        print("\n  [ERROR] xlrd no está instalado.")
        print("  Los archivos .xls de INEGI (Excel 97-2003) requieren xlrd.")
        print("  Instala con UNO de estos comandos:")
        print("    conda install -c conda-forge xlrd")
        print("    pip install xlrd")
        sys.exit(1)

def get_engine(path: Path) -> str:
    """Siempre xlrd para .xls, openpyxl para .xlsx."""
    if path.suffix.lower() == ".xls":
        return "xlrd"
    return "openpyxl"

def read_sheet(path: Path, sheet, nrows=None) -> pd.DataFrame:
    engine = get_engine(path)
    return pd.read_excel(path, sheet_name=sheet,
                         header=None, nrows=nrows, engine=engine)

def get_sheet_names(path: Path) -> list:
    engine = get_engine(path)
    try:
        return pd.ExcelFile(path, engine=engine).sheet_names
    except Exception as e:
        raise RuntimeError(f"{path.name}: {e}") from e

def sanity_check(xls_files: list):
    """Lee el primer XLS y muestra las primeras filas no-vacías como diagnóstico."""
    f = xls_files[0]
    print(f"\n  [SANITY] Verificando lectura: {f.name}")
    try:
        sheets = get_sheet_names(f)
        sheet  = next((s for s in sheets if re.search(r"total", s, re.I)), sheets[0])
        df = read_sheet(f, sheet, nrows=15)
        print(f"  Shape: {df.shape}  /  hoja: {sheet}")
        for i in range(min(15, len(df))):
            vals = [str(v) for v in df.iloc[i].tolist()
                    if str(v) not in ("nan","None","<NA>","")]
            if vals:
                print(f"    fila {i:2d}: {vals[:5]}")
    except Exception as e:
        print(f"  [ERROR en sanity]: {e}")
        sys.exit(1)
    print()


# ============================================================
# PERIODO
# ============================================================
def sheet_to_periodo(sheet_name: str):
    m = re.search(r"total[_\s-]*([1-4])(\d{2})", sheet_name, flags=re.IGNORECASE)
    if not m: return None
    return f"{2000 + int(m.group(2))}T{int(m.group(1))}"


# ============================================================
# PARSER XLS
# ============================================================
def parse_xls_nacional(path: Path, sheet: str) -> dict:
    try:
        df = read_sheet(path, sheet)   # lee hoja completa sin límite de filas
    except Exception as e:
        return {"error": str(e)}

    # Tasas explícitas (pueden estar en cualquier fila de la hoja)
    tpart  = find_label_get_total(df, LABELS_TPART)
    tdesoc = find_label_get_total(df, LABELS_TDESOC)

    # Normaliza si vienen en % (ej. 59.8 → 0.598)
    if tpart  is not None and tpart  > 1.5: tpart  /= 100
    if tdesoc is not None and tdesoc > 1.5: tdesoc /= 100

    # Niveles (siempre presentes en filas ~10-14)
    p15   = find_label_get_total(df, LABELS_P15)
    pea   = find_label_get_total(df, LABELS_PEA)
    desoc = find_label_get_total(df, LABELS_DESOC)

    tpart_calc  = (pea / p15)   if (p15   and pea   and p15  > 0) else None
    tdesoc_calc = (desoc / pea) if (pea   and desoc and pea  > 0) else None

    return {
        "TPART":           tpart  if tpart  is not None else tpart_calc,
        "TDESOC":          tdesoc if tdesoc is not None else tdesoc_calc,
        "tpart_explicit":  tpart,
        "tdesoc_explicit": tdesoc,
        "tpart_calc":      tpart_calc,
        "tdesoc_calc":     tdesoc_calc,
        "P15MAS":          p15,
        "PEA":             pea,
        "DESOC":           desoc,
    }


# ============================================================
# LOCALIZAR Y PARSEAR XLS
# ============================================================
def find_nacional_xls(valid_dir: Path) -> list:
    found = []
    for ext in ("*.xls", "*.xlsx"):
        for f in valid_dir.rglob(ext):
            if "nacional" in f.name.lower():
                found.append(f)
    print(f"  XLS nacionales encontrados: {len(found)}")
    return sorted(set(found))


def parse_official_tidy(valid_dir: Path) -> pd.DataFrame:
    xls_files = find_nacional_xls(valid_dir)
    if not xls_files:
        print(f"  [ERROR] Sin XLS en {valid_dir}")
        return pd.DataFrame()

    # Verificación rápida antes de iterar 63 archivos
    sanity_check(xls_files)

    rows = []
    n_explicit = n_calc = n_err = 0

    for f in xls_files:
        try:
            sheets = get_sheet_names(f)
        except RuntimeError as e:
            print(f"  [WARN] {e}"); n_err += 1; continue

        for sheet in sheets:
            periodo = sheet_to_periodo(sheet)
            if periodo is None: continue

            result = parse_xls_nacional(f, sheet)

            if "error" in result:
                print(f"  [WARN] {f.name}/{sheet}: {result['error']}")
                n_err += 1; continue

            tpart  = result["TPART"]
            tdesoc = result["TDESOC"]

            if tpart is None and tdesoc is None:
                print(f"  [WARN] {f.name}/{sheet}: sin valores "
                      f"— P15={result['P15MAS']} PEA={result['PEA']} DESOC={result['DESOC']}")
                continue

            if result["tpart_explicit"] is not None: n_explicit += 1
            else:                                    n_calc    += 1

            rows.append({
                "periodo":        periodo,
                "TPART_oficial":  tpart,
                "TDESOC_oficial": tdesoc,
                "metodo":         "explicito" if result["tpart_explicit"] else "calculado",
                "P15MAS_oficial": result["P15MAS"],
                "PEA_oficial":    result["PEA"],
                "DESOC_oficial":  result["DESOC"],
                "fuente":         f.name,
            })

    print(f"  Periodos parseados : {len(rows)}")
    print(f"    tasas explícitas : {n_explicit}")
    print(f"    calculadas       : {n_calc}")
    if n_err: print(f"    errores          : {n_err}")

    if not rows: return pd.DataFrame()

    tidy = pd.DataFrame(rows)
    tidy["_nna"] = tidy[["TPART_oficial","TDESOC_oficial"]].notna().sum(axis=1)
    tidy = (tidy.sort_values(["periodo","_nna"], ascending=[True,False])
               .drop_duplicates("periodo", keep="first")
               .drop(columns=["_nna"])
               .reset_index(drop=True))
    return tidy


# ============================================================
# MICRO
# ============================================================
def pick_col(df, candidates):
    cols_up = {c.upper(): c for c in df.columns}
    for c in candidates:
        if c.upper() in cols_up: return cols_up[c.upper()]
    return None

def compute_micro(path: Path) -> dict:
    df = pd.read_parquet(path)
    fac    = pick_col(df, ["FAC","FAC_P","FACTOR"])
    edad   = pick_col(df, ["EDA","EDAD"])
    clase1 = pick_col(df, ["CLASE1"])
    clase2 = pick_col(df, ["CLASE2"])
    if not all([fac, edad, clase1]):
        return {"error": f"Faltan: FAC={fac} EDA={edad} CLASE1={clase1}"}
    w  = pd.to_numeric(df[fac],    errors="coerce").fillna(0)
    e  = pd.to_numeric(df[edad],   errors="coerce")
    c1 = pd.to_numeric(df[clase1], errors="coerce")
    c2 = pd.to_numeric(df[clase2], errors="coerce") if clase2 else pd.Series(np.nan, index=df.index)
    m15  = e  >= 15
    mpea = m15 & (c1 == 1)
    mdes = mpea & (c2 == 2)
    p15  = float(w[m15].sum())
    pea  = float(w[mpea].sum())
    des  = float(w[mdes].sum())
    return {
        "P15MAS_micro": p15, "PEA_micro": pea, "DESOC_micro": des,
        "TPART_micro":  pea/p15 if p15>0 else np.nan,
        "TDESOC_micro": des/pea if pea>0 else np.nan,
        "N_filas": len(df),
    }

def semaforo(d, u):
    if pd.isna(d): return "???"
    a = abs(d)
    if a <= u:     return "OK "
    elif a <= u*3: return "AVG"
    return "ERR"


# ============================================================
# AUTO-DETECT BASE_DIR
# ============================================================
def resolve_base_dir(cli_arg=None) -> Path:
    if cli_arg: return Path(cli_arg).resolve()
    if BASE_DIR_MANUAL: return Path(BASE_DIR_MANUAL).resolve()
    for c in [Path(__file__).resolve().parent,
              Path(__file__).resolve().parent.parent,
              Path(__file__).resolve().parent.parent.parent]:
        if (c / "derivados").exists(): return c
    return Path.cwd()


# ============================================================
# MAIN
# ============================================================
def main():
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--base-dir", default=None)
    args, _ = ap.parse_known_args()

    base_dir  = resolve_base_dir(args.base_dir)
    parquet_d = base_dir / "derivados"
    valid_d   = base_dir / "validadores_2" / "extract" / "trad"
    out_dir   = base_dir / "diagnosticos"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 62)
    print("  04T_validacion_v6.py — Micro vs INEGI (Tradicional)")
    print("=" * 62)
    print(f"  BASE_DIR : {base_dir}")
    print(f"  VALID_DIR: {valid_d}")

    # Verificar xlrd antes de empezar
    require_xlrd()
    print("  xlrd: OK  (forzado para .xls)")

    # ── Micro ─────────────────────────────────────────────────
    parquets = sorted(parquet_d.glob("enoe_trad_*_T*.parquet"))
    if not parquets: parquets = sorted(parquet_d.glob("enoe_trad_*_T*.csv.gz"))
    if not parquets:
        print(f"\n  [ERROR] Sin parquets en {parquet_d}"); return

    print(f"\n  Parquets: {len(parquets)}")
    micro_rows = []
    for f in parquets:
        m = re.search(r"enoe_trad_(\d{4})_T([1-4])", f.name, re.IGNORECASE)
        if not m: continue
        periodo = f"{m.group(1)}T{m.group(2)}"
        print(f"  micro {periodo}...", end="  ")
        stats = compute_micro(f); stats["periodo"] = periodo
        micro_rows.append(stats)
        if "error" in stats: print(f"[WARN] {stats['error']}")
        else: print(f"TPART={stats['TPART_micro']:.4f}  TDESOC={stats['TDESOC_micro']:.4f}")
    micro_df = pd.DataFrame(micro_rows)

    # ── Oficial ───────────────────────────────────────────────
    print(f"\n  Parseando oficiales en {valid_d} ...")
    if not valid_d.exists():
        print(f"  [ERROR] No existe: {valid_d}")
        micro_df.to_csv(out_dir/"04T_micro_sin_oficial.csv",
                        index=False, encoding="utf-8-sig"); return

    oficial_df = parse_official_tidy(valid_d)
    if oficial_df.empty:
        micro_df.to_csv(out_dir/"04T_micro_sin_oficial.csv",
                        index=False, encoding="utf-8-sig"); return

    # ── Merge ─────────────────────────────────────────────────
    df = micro_df.merge(oficial_df, on="periodo", how="left")
    df["diff_TPART_pp"]  = (df["TPART_micro"]  - df["TPART_oficial"])  * 100
    df["diff_TDESOC_pp"] = (df["TDESOC_micro"]  - df["TDESOC_oficial"]) * 100
    df["sem_TPART"]  = df["diff_TPART_pp"].map(lambda x: semaforo(x, UMBRAL_PP))
    df["sem_TDESOC"] = df["diff_TDESOC_pp"].map(lambda x: semaforo(x, UMBRAL_PP))

    out_csv = out_dir / "04T_validacion_trad.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    n_total  = len(df)
    n_sin_of = df["TPART_oficial"].isna().sum()
    n_con_of = n_total - n_sin_of

    lines = [
        "=" * 62, "  04T VALIDACION — Resumen", "=" * 62,
        f"  Trimestres micro      : {n_total}",
        f"  Con oficial           : {n_con_of}",
        f"  Sin oficial           : {n_sin_of}",
        f"  TPART  OK (<={UMBRAL_PP}pp): {(df['sem_TPART'] =='OK ').sum()} / {n_con_of}",
        f"  TDESOC OK (<={UMBRAL_PP}pp): {(df['sem_TDESOC']=='OK ').sum()} / {n_con_of}",
    ]
    for lbl, col in [("TPART","diff_TPART_pp"),("TDESOC","diff_TDESOC_pp")]:
        s = df[col].dropna()
        if len(s):
            lines += ["", f"  Diff {lbl} (pp):",
                      f"    media  : {s.mean():.3f}",
                      f"    mediana: {s.median():.3f}",
                      f"    max abs: {s.abs().max():.3f}"]

    lines += ["",
              f"  {'Periodo':10s} {'TPART_m':8s} {'TPART_of':9s} {'dif_pp':7s} {'sem':4s}"
              f"  {'TDESOC_m':9s} {'TDESOC_of':10s} {'dif_pp':7s} {'sem':4s}"]
    for _, r in df.sort_values("periodo").iterrows():
        tp = f"{r['TPART_oficial']:.4f}"  if pd.notna(r['TPART_oficial'])  else "  N/A  "
        td = f"{r['TDESOC_oficial']:.4f}" if pd.notna(r['TDESOC_oficial']) else "  N/A  "
        dp = f"{r['diff_TPART_pp']:+.2f}" if pd.notna(r['diff_TPART_pp'])  else "  N/A"
        dd = f"{r['diff_TDESOC_pp']:+.2f}"if pd.notna(r['diff_TDESOC_pp']) else "  N/A"
        lines.append(
            f"  {r['periodo']:10s} {r['TPART_micro']:.4f}   {tp}   {dp:7s} {r['sem_TPART']:4s}"
            f"  {r['TDESOC_micro']:.4f}    {td}    {dd:7s} {r['sem_TDESOC']:4s}"
        )

    txt = "\n".join(lines)
    print("\n" + txt)
    (out_dir / "04T_resumen_trad.txt").write_text(txt, encoding="utf-8")
    print(f"\n  OK -> {out_csv}")
    print(f"  OK -> {out_dir}/04T_resumen_trad.txt")

if __name__ == "__main__":
    main()