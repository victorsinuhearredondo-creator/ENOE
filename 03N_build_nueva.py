#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03N_build_nueva.py
==================
Constructor Nueva ENOE (2023T1 - 2025T4): DBF -> Parquet.

Diferencias clave respecto a 03T (tradicional):
  - Factor de expansion: FAC_TRI (no FAC)
  - Tamano localidad: T_LOC_TRI + T_LOC_MEN (no T_LOC unico)
  - Estrato diseno: EST_D_TRI (no EST_D)
  - Variables geograficas nuevas: CVEGEO, CVE_ENT, TIPOLEV
  - La llave del hogar puede incluir N_PRO_VIV ademas de los 6 canonicos

IDs canonicos (identicos al 03T para que los scripts de analisis
funcionen con ambas series sin cambios):
  ID_HOGAR   = CD_A|ENT|CON|UPM|V_SEL|N_HOG (|N_PRO_VIV si existe)
  ID_ROSTER  = N_REN
  ID_PERSONA = ID_HOGAR|ID_ROSTER

Nota sobre ENOEN (2020T3-2022T4):
  Serie excluida del analisis por quiebre metodologico COVID-19
  (levantamiento telefonico CATI, marco muestral distinto).
  Los DBF estan descargados pero este script no los procesa.

Como correr desde Spyder:
  1. Cambia BASE_DIR (unica linea a editar).
  2. Presiona F5.

Requisitos: pandas, pyarrow (o fallback csv.gz sin pyarrow).
"""

import datetime as _dt
import os
import re
import struct
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# ============================================================
# -- CONFIGURACION --- UNICA SECCION A EDITAR ----------------
# ============================================================

BASE_DIR = r"C:\Users\vicou\OneDrive\Documentos\00_RESPALDO_VS\00_Labor_Market\ENOE"

# Rango de la nueva ENOE
START_YEAR  = 2023
END_YEAR    = 2025
END_QUARTER = 4      # actualizar si hay nueva publicacion

# Si True -> reconstruye aunque el parquet ya exista
FORCE_REBUILD = False

# Encoding de los DBF del INEGI
DBF_ENCODING = "latin-1"

# -- Rutas derivadas (no editar) -----------------------------
RAW_DIR  = str(Path(BASE_DIR) / "nueva")
OUT_DIR  = str(Path(BASE_DIR) / "derivados_nueva")
MAPA_CSV = str(Path(BASE_DIR) / "diagnosticos" / "mapa_homologacion.csv")

# ============================================================
# -- DIFERENCIAS CLAVE NUEVA vs TRADICIONAL ------------------
# ============================================================
#
# Variable          Tradicional     Nueva ENOE
# ------------------------------------------------
# Factor expansion  FAC             FAC_TRI (trimestral)
#                                   FAC_MEN (mensual, si se necesita)
# Tamano localidad  T_LOC           T_LOC_TRI + T_LOC_MEN
# Estrato diseno    EST_D           EST_D_TRI
# Claves geo        (no existen)    CVEGEO, CVE_ENT, TIPOLEV
# Llave hogar       6 campos        6 campos + N_PRO_VIV (si existe)
#
# Estas diferencias estan documentadas en mapa_homologacion.csv
# (columna alias_nueva_encontrado) y en el scan_dbf_reporte.html.
#
# ============================================================

# Llaves hogar en orden de prioridad
# N_PRO_VIV solo existe en nueva; si esta, se agrega a la llave
HOG_KEY_CANDIDATES_BASE = ["CD_A", "ENT", "CON", "UPM", "V_SEL", "N_HOG"]
HOG_KEY_EXTRA_NUEVA     = ["N_PRO_VIV"]

# Candidatos de roster (mismo orden que 03T para consistencia)
_ROSTER_SEARCH = ["N_REN", "RENGLON", "NUMREN", "NUM_REN", "NREN", "R_REN", "ID_ROSTER"]

# Homologacion especifica de la nueva ENOE
# (lo que el mapa detecta en alias_nueva_encontrado)
MAPA_NUEVA_DEFECTO = {
    "FAC_P":     ["FAC_TRI", "FAC_MEN", "FAC"],
    "T_LOC":     ["T_LOC_TRI", "T_LOC_MEN", "T_LOC"],
    "EST_D":     ["EST_D_TRI", "EST_D"],
    "NIV_INS":   ["NIV_INS"],
    "PAR_C":     ["PAR_C"],
    "BUSCAR5C":  ["BUSCAR5C"],
    "DISPO":     ["DISPO"],
    "NODISPO":   ["NODISPO"],
    # ENT puede venir como ENT o CVE_ENT en nueva
    "ENT":       ["ENT", "CVE_ENT"],
}


# ============================================================
# -- LECTOR DE MAPA DE HOMOLOGACION -------------------------
# ============================================================

def load_mapa(mapa_path: str) -> Dict[str, List[str]]:
    """
    Lee mapa_homologacion.csv usando la columna alias_nueva_encontrado
    (distinto a 03T que usa alias_trad_encontrado).
    """
    if not os.path.exists(mapa_path):
        print("  [WARN] mapa_homologacion.csv no encontrado.")
        print("    Usando mapa por defecto. Corre 02_scan_dbf.py primero.")
        return MAPA_NUEVA_DEFECTO.copy()

    try:
        df = pd.read_csv(mapa_path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(mapa_path, encoding="latin-1")

    # Busca columna de aliases para nueva
    col_nueva = None
    for c in df.columns:
        if "nueva" in c.lower() and "encontrado" in c.lower():
            col_nueva = c
            break
    if col_nueva is None:
        print("  [WARN] No encontre columna alias_nueva_encontrado en el mapa.")
        print("    Usando mapa por defecto.")
        return MAPA_NUEVA_DEFECTO.copy()

    mapa = {}
    for _, row in df.iterrows():
        canonico = str(row.get("nombre_canonico", "")).strip()
        encontrado = str(row.get(col_nueva, "")).strip()
        if not canonico or encontrado in ("", "---", "nan"):
            continue
        aliases = [a.strip() for a in encontrado.split("/") if a.strip()]
        if aliases:
            mapa[canonico] = aliases

    # Agrega entradas del mapa por defecto que no esten en el CSV
    for k, v in MAPA_NUEVA_DEFECTO.items():
        if k not in mapa:
            mapa[k] = v

    print("  Mapa cargado: " + str(len(mapa)) + " variables canonicas (columna nueva).")
    return mapa


def find_alias_in_columns(columns: List[str], aliases: List[str]) -> Optional[str]:
    col_upper = {c.upper(): c for c in columns}
    for alias in aliases:
        if alias.upper() in col_upper:
            return col_upper[alias.upper()]
    return None


# ============================================================
# -- LECTOR DBF (identico a 03T) ----------------------------
# ============================================================

def read_dbf(path: Path, encoding: str = "latin-1") -> pd.DataFrame:
    with path.open("rb") as f:
        header = f.read(32)
        if len(header) < 32:
            raise ValueError("DBF invalido (header corto): " + str(path))

        num_records = struct.unpack("<I", header[4:8])[0]
        header_len  = struct.unpack("<H", header[8:10])[0]
        record_len  = struct.unpack("<H", header[10:12])[0]

        fields = []
        f.seek(32)
        while True:
            desc = f.read(32)
            if not desc or desc[0] == 0x0D:
                break
            raw  = desc[0:11].split(b"\x00", 1)[0]
            name = raw.decode(encoding, errors="ignore").strip().upper()
            ftype    = chr(desc[11])
            length   = desc[16]
            decimals = desc[17]
            if name:
                fields.append((name, ftype, length, decimals))

        f.seek(header_len)
        rows = []

        for _ in range(num_records):
            rec = f.read(record_len)
            if not rec or len(rec) < record_len:
                break
            if rec[0:1] == b"*":
                continue

            row = []
            pos = 1
            for name, ftype, length, decimals in fields:
                raw = rec[pos: pos + length]
                pos += length

                if ftype == "C":
                    val = raw.decode(encoding, errors="ignore").strip()
                    row.append(val if val != "" else pd.NA)
                elif ftype in ("N", "F", "I"):
                    s = raw.decode(encoding, errors="ignore").strip().replace(",", "")
                    if s in ("", "."):
                        row.append(pd.NA)
                    else:
                        try:
                            row.append(int(float(s)) if not (decimals and "." in s) else float(s))
                        except Exception:
                            try:
                                row.append(float(s))
                            except Exception:
                                row.append(pd.NA)
                elif ftype == "D":
                    s = raw.decode(encoding, errors="ignore").strip()
                    if len(s) == 8 and s.isdigit():
                        try:
                            row.append(_dt.date(int(s[:4]), int(s[4:6]), int(s[6:])))
                        except Exception:
                            row.append(pd.NA)
                    else:
                        row.append(pd.NA)
                elif ftype == "L":
                    s = raw.decode(encoding, errors="ignore").strip().upper()
                    row.append(True if s in ("Y", "T") else False if s in ("N", "F") else pd.NA)
                else:
                    val = raw.decode(encoding, errors="ignore").strip()
                    row.append(val if val != "" else pd.NA)

            rows.append(row)

    return pd.DataFrame(rows, columns=[f[0] for f in fields])


# ============================================================
# -- BUSQUEDA DE DBF -----------------------------------------
# ============================================================

def periodo_suf_nueva(year: int, quarter: int) -> str:
    """
    La nueva ENOE usa el mismo patron de sufijo numerico que la tradicional:
    trimestre + 2 digitos del anno.  Ej: 2023T1 -> '123', 2024T3 -> '324'
    """
    return str(quarter) + str(year % 100).zfill(2)


def build_index(rawroot: Path) -> Dict[Tuple[str, str], Path]:
    idx: Dict[Tuple[str, str], Path] = {}
    rx = re.compile(r"(?:ENOE_)?([A-Z0-9]+?)(\d{3})\.DBF$", re.IGNORECASE)
    for p in list(rawroot.rglob("*.dbf")) + list(rawroot.rglob("*.DBF")):
        m = rx.search(p.name.upper())
        if not m:
            continue
        mod = m.group(1).upper()
        suf = m.group(2)
        mod = re.sub(r"^(COE[12])T$", r"\1", mod)
        idx[(mod, suf)] = p
    return idx


def pick_dbf(idx: Dict[Tuple[str, str], Path],
             modulo: str, suf: str) -> Optional[Path]:
    modulo = modulo.upper()
    if modulo in ("COE1", "COE2"):
        for v in (modulo + "T", modulo):
            if (v, suf) in idx:
                return idx[(v, suf)]
        return None
    return idx.get((modulo, suf))


# ============================================================
# -- CONSTRUCCION DE IDs -------------------------------------
# ============================================================

def build_id_roster(df: pd.DataFrame) -> pd.Series:
    out = pd.Series([pd.NA] * len(df), dtype="Int64", index=df.index)
    for c in _ROSTER_SEARCH:
        if c in df.columns:
            out = out.fillna(pd.to_numeric(df[c], errors="coerce").astype("Int64"))
    return out


def build_hog_id(df: pd.DataFrame, keys: List[str]) -> pd.Series:
    parts = [df[k].astype("string").fillna("") for k in keys]
    out = parts[0]
    for p in parts[1:]:
        out = out + "|" + p
    return out


def dedup_on(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    valid = [k for k in keys if k in df.columns]
    if not valid or not df.duplicated(subset=valid).any():
        return df
    n_before = len(df)
    df = df.drop_duplicates(subset=valid, keep="first")
    print("    dedup " + str(valid) + ": " + str(n_before) + " -> " + str(len(df)))
    return df


# ============================================================
# -- HOMOLOGACION --------------------------------------------
# ============================================================

def homologate(df: pd.DataFrame, mapa: Dict[str, List[str]]) -> pd.DataFrame:
    cols = list(df.columns)
    for canonico, aliases in mapa.items():
        if canonico in df.columns:
            continue
        found = find_alias_in_columns(cols, aliases)
        if found:
            df[canonico] = df[found]
    return df


# ============================================================
# -- CONSTRUCCION DE UN TRIMESTRE ----------------------------
# ============================================================

def build_quarter(year: int, quarter: int,
                  idx: Dict[Tuple[str, str], Path],
                  outdir: Path,
                  mapa: Dict[str, List[str]],
                  encoding: str) -> bool:

    label   = str(year) + "T" + str(quarter)
    suf     = periodo_suf_nueva(year, quarter)
    out_pq  = outdir / ("enoe_nueva_" + str(year) + "_T" + str(quarter) + ".parquet")
    out_csv = outdir / ("enoe_nueva_" + str(year) + "_T" + str(quarter) + ".csv.gz")

    # -- Skip si ya existe -----------------------------------
    if not FORCE_REBUILD and (out_pq.exists() or out_csv.exists()):
        existing = out_pq.name if out_pq.exists() else out_csv.name
        print("  " + label + " -> ya existe (" + existing + "), omitiendo.")
        return True

    print("")
    print("  " + "=" * 50)
    print("  Procesando: " + label + "  (suf=" + suf + ")")
    print("  " + "=" * 50)

    # -- Localizar DBFs --------------------------------------
    f_sdem = pick_dbf(idx, "SDEMT", suf)
    f_hog  = pick_dbf(idx, "HOGT",  suf)
    f_viv  = pick_dbf(idx, "VIVT",  suf)

    print("  SDEMT : " + (f_sdem.name if f_sdem else "[NO ENCONTRADO]"))
    print("  HOGT  : " + (f_hog.name  if f_hog  else "[NO ENCONTRADO]"))
    print("  VIVT  : " + (f_viv.name  if f_viv  else "(no disponible)"))

    if f_sdem is None or f_hog is None:
        print("  [WARN] Falta SDEMT o HOGT -> omito " + label)
        return False

    # -- Leer DBFs -------------------------------------------
    print("  Leyendo SDEMT...", end=" ")
    df_s = read_dbf(f_sdem, encoding=encoding)
    print(str(len(df_s)) + " filas, " + str(len(df_s.columns)) + " cols")

    print("  Leyendo HOGT...", end=" ")
    df_h = read_dbf(f_hog, encoding=encoding)
    print(str(len(df_h)) + " filas, " + str(len(df_h.columns)) + " cols")

    df_v = pd.DataFrame()
    if f_viv:
        print("  Leyendo VIVT...", end=" ")
        df_v = read_dbf(f_viv, encoding=encoding)
        print(str(len(df_v)) + " filas, " + str(len(df_v.columns)) + " cols")

    # -- Reporte de columnas disponibles en HOGT (util para diagnostico) --
    hog_cols = list(df_h.columns)
    nuevas_hogt = [c for c in ["CVEGEO","CVE_ENT","TIPOLEV","N_PRO_VIV","FAC_TRI","FAC_MEN","T_LOC_TRI","T_LOC_MEN","EST_D_TRI"] if c in hog_cols]
    if nuevas_hogt:
        print("  Variables nuevas en HOGT: " + str(nuevas_hogt))

    # -- Homologar ENT <- CVE_ENT ANTES de construir llaves --
    # La nueva ENOE no incluye ENT en SDEMT; usa CVE_ENT en su lugar.
    # Si no hacemos esto aqui, ENT queda fuera del ID_HOGAR.
    if "ENT" not in df_s.columns and "CVE_ENT" in df_s.columns:
        df_s["ENT"] = df_s["CVE_ENT"]
        print("  ENT <- CVE_ENT (homologado en SDEMT antes de construir llaves)")
    if "ENT" not in df_h.columns and "CVE_ENT" in df_h.columns:
        df_h["ENT"] = df_h["CVE_ENT"]
        print("  ENT <- CVE_ENT (homologado en HOGT)")
    if not df_v.empty:
        if "ENT" not in df_v.columns and "CVE_ENT" in df_v.columns:
            df_v["ENT"] = df_v["CVE_ENT"]

    # -- Llaves del hogar ------------------------------------
    # Para la nueva ENOE se agrega N_PRO_VIV si existe en SDEMT
    keys_hog = [k for k in HOG_KEY_CANDIDATES_BASE if k in df_s.columns]

    # N_PRO_VIV como llave extra si existe en ambos
    for extra in HOG_KEY_EXTRA_NUEVA:
        if extra in df_s.columns and extra in df_h.columns:
            keys_hog.append(extra)
            print("  Llave extra detectada: " + extra)

    if not keys_hog or "N_HOG" not in keys_hog:
        print("  [ERROR] No se encontraron llaves de hogar en SDEMT.")
        print("    Cols disponibles: " + str(list(df_s.columns[:20])))
        return False

    if "ENT" not in keys_hog:
        print("  [WARN] ENT no esta en las llaves del hogar. Revisa el DBF.")

    print("  Llaves hogar: " + str(keys_hog))

    # -- Construir IDs en SDEMT ------------------------------
    df_s["ID_HOGAR"]  = build_hog_id(df_s, keys_hog)
    df_s["ID_ROSTER"] = build_id_roster(df_s)

    if df_s["ID_ROSTER"].isna().any():
        n_miss = int(df_s["ID_ROSTER"].isna().sum())
        found_roster = [c for c in _ROSTER_SEARCH if c in df_s.columns]
        print("  [WARN] ID_ROSTER: " + str(n_miss) + " NAs. Cols: " + str(found_roster))

    df_s["ID_PERSONA"] = (
        df_s["ID_HOGAR"].astype("string") + "|" +
        df_s["ID_ROSTER"].astype("string")
    )

    # -- Merge HOGT ------------------------------------------
    keys_hog_hog = [k for k in keys_hog if k in df_h.columns]
    df_h2 = dedup_on(df_h, keys_hog_hog)
    n_before = len(df_s)
    df = df_s.merge(df_h2, on=keys_hog_hog, how="left", suffixes=("", "_HOG"))
    if len(df) != n_before:
        print("  [WARN] Merge HOGT cambio filas: " + str(n_before) + " -> " + str(len(df)))
    print("  Merge HOGT OK: " + str(len(keys_hog_hog)) + " llaves, " + str(len(df_h2)) + " hogares")

    # -- Merge VIVT ------------------------------------------
    if not df_v.empty:
        # VIVT usa llaves de vivienda (sin N_HOG, sin N_PRO_VIV)
        viv_key_candidates = ["CD_A", "ENT", "CON", "UPM", "V_SEL"]
        keys_viv = [k for k in viv_key_candidates if k in df.columns and k in df_v.columns]
        if keys_viv:
            df_v2 = dedup_on(df_v, keys_viv)
            n_before = len(df)
            df = df.merge(df_v2, on=keys_viv, how="left", suffixes=("", "_VIV"))
            if len(df) != n_before:
                print("  [WARN] Merge VIVT cambio filas: " + str(n_before) + " -> " + str(len(df)))
            print("  Merge VIVT OK: " + str(len(keys_viv)) + " llaves")

    # -- Homologar variables canonicas -----------------------
    df = homologate(df, mapa)

    # -- Validaciones ----------------------------------------
    n_hog  = int(df["ID_HOGAR"].nunique(dropna=True))
    n_pers = len(df)
    pph    = n_pers / n_hog if n_hog else float("nan")
    dup_id = int(df.duplicated(subset=["ID_HOGAR", "ID_ROSTER"]).sum())

    rph = df.groupby("ID_HOGAR")["ID_ROSTER"].nunique(dropna=True)
    print("  Hogares: " + str(n_hog) +
          "   Personas: " + str(n_pers) +
          "   P/hogar: " + str(round(pph, 2)))
    print("  Roster/hogar: min=" + str(int(rph.min())) +
          " med=" + str(round(float(rph.median()), 1)) +
          " max=" + str(int(rph.max())))
    if dup_id > 0:
        print("  [WARN] Duplicados (ID_HOGAR, ID_ROSTER): " + str(dup_id))
    else:
        print("  Unicidad ID_PERSONA: OK")

    # Reporte de variables canonicas nueva-especificas
    canon_check = ["ENT", "FAC_P", "NIV_INS", "ANIOS_ESC", "PAR_C",
                   "EDA", "SEX", "HRSOCUP", "INGOCUP", "CLASE1",
                   "BUSCAR5C", "DISPO", "RAMA", "ZONA",
                   "T_LOC", "EST_D",   # nombres canonicos homologados
                   "CVEGEO", "TIPOLEV"]  # nuevas solo en nueva ENOE
    present = [v for v in canon_check if v in df.columns]
    absent  = [v for v in canon_check if v not in df.columns]
    print("  Vars OK : " + str(present))
    if absent:
        print("  Vars --- : " + str(absent))

    # -- Exportar --------------------------------------------
    outdir.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_pq, index=False)
        print("  OK -> " + out_pq.name)
        return True
    except Exception as e:
        print("  [WARN] parquet fallo (" + str(e) + "), usando CSV.gz...")
        df.to_csv(out_csv, index=False, compression="gzip", encoding="utf-8")
        print("  OK (fallback) -> " + out_csv.name)
        return True


# ============================================================
# -- MAIN ----------------------------------------------------
# ============================================================

def main() -> None:
    print("=" * 60)
    print("  03N_build_nueva.py")
    print("  Constructor Nueva ENOE  DBF -> Parquet")
    print("=" * 60)
    print("  BASE_DIR  : " + BASE_DIR)
    print("  RAW_DIR   : " + RAW_DIR)
    print("  OUT_DIR   : " + OUT_DIR)
    print("  MAPA_CSV  : " + MAPA_CSV)
    print("  Rango     : " + str(START_YEAR) + "T1 - " + str(END_YEAR) + "T" + str(END_QUARTER))
    print("  FORCE     : " + str(FORCE_REBUILD))
    print("")
    print("  ENOEN (2020T3-2022T4): excluida por quiebre metodologico COVID-19.")

    rawroot = Path(RAW_DIR)
    outdir  = Path(OUT_DIR)

    if not rawroot.exists():
        print("")
        print("  [ERROR] RAW_DIR no existe: " + str(rawroot))
        print("  Verifica BASE_DIR y que los DBF esten en nueva/<anno>/T<n>/")
        return

    # -- Mapa de homologacion --------------------------------
    print("")
    print("[1/3] Cargando mapa de homologacion (columna nueva)...")
    mapa = load_mapa(MAPA_CSV)

    # -- Indexar DBFs ----------------------------------------
    print("")
    print("[2/3] Indexando DBFs en " + RAW_DIR + " ...")
    idx = build_index(rawroot)
    print("  " + str(len(idx)) + " DBFs indexados.")

    if not idx:
        print("  [ERROR] No se encontraron DBFs.")
        return

    # -- Diagnostico rapido de sufijos disponibles -----------
    sufs_sdemt = sorted(set(suf for (mod, suf), _ in idx.items() if mod == "SDEMT"))
    print("  Sufijos SDEMT detectados: " + str(sufs_sdemt))

    # -- Procesar trimestres ---------------------------------
    print("")
    print("[3/3] Construyendo parquets...")

    ok = skip = fail = 0

    for year in range(START_YEAR, END_YEAR + 1):
        for quarter in range(1, 5):
            if year == END_YEAR and quarter > END_QUARTER:
                continue

            suf = periodo_suf_nueva(year, quarter)
            if not (pick_dbf(idx, "SDEMT", suf) or pick_dbf(idx, "HOGT", suf)):
                continue

            out_pq  = outdir / ("enoe_nueva_" + str(year) + "_T" + str(quarter) + ".parquet")
            out_csv = outdir / ("enoe_nueva_" + str(year) + "_T" + str(quarter) + ".csv.gz")
            already = (not FORCE_REBUILD) and (out_pq.exists() or out_csv.exists())

            try:
                result = build_quarter(year, quarter, idx, outdir, mapa, DBF_ENCODING)
                if result:
                    if already:
                        skip += 1
                    else:
                        ok += 1
                else:
                    fail += 1
            except Exception as e:
                import traceback
                print("  [ERROR] Fallo " + str(year) + "T" + str(quarter) + ": " + str(e))
                traceback.print_exc()
                fail += 1

    # -- Resumen ---------------------------------------------
    print("")
    print("=" * 60)
    print("  RESUMEN")
    print("=" * 60)
    print("  Construidos (nuevos)  : " + str(ok))
    print("  Omitidos (ya existen) : " + str(skip))
    print("  Fallidos              : " + str(fail))
    print("  Salida en             : " + OUT_DIR)
    if fail > 0:
        print("")
        print("  [ATENCION] Revisa mensajes [ERROR] y [WARN] arriba.")
        print("  Si ves 'NO ENCONTRADO' para muchos trimestres, verifica")
        print("  que el patron del sufijo numerico sea correcto para la nueva ENOE.")
    print("=" * 60)


if __name__ == "__main__":
    main()
