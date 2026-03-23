#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03T_build_trad.py
=================
Constructor ENOE Tradicional (2005T1 - 2020T1): DBF -> Parquet.

Diferencia clave respecto al constructor anterior (v8):
  Lee el archivo mapa_homologacion.csv generado por 02_scan_dbf.py
  para saber exactamente como se llama cada variable en cada trimestre.
  Esto evita tener los aliases hardcodeados en el codigo.

Fuentes por trimestre (carpeta tradicional/<anno>/T<n>/):
  - SDEMT  : personas + variables analiticas
  - HOGT   : caracteristicas del hogar
  - VIVT   : caracteristicas de la vivienda (opcional)

IDs canonicos del pipeline:
  ID_HOGAR   = CD_A|ENT|CON|UPM|V_SEL|N_HOG (|H_MUD si existe)
  ID_ROSTER  = N_REN (o el alias que haya en ese trimestre)
  ID_PERSONA = ID_HOGAR|ID_ROSTER

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

# Rango de la serie tradicional
START_YEAR  = 2005
END_YEAR    = 2020
END_QUARTER = 1      # la tradicional termina en 2020T1

# Si True -> reconstruye aunque el parquet ya exista
FORCE_REBUILD = False

# Encoding de los DBF del INEGI
DBF_ENCODING = "latin-1"

# -- Rutas derivadas (no editar) -----------------------------
RAW_DIR  = str(Path(BASE_DIR) / "tradicional")
OUT_DIR  = str(Path(BASE_DIR) / "derivados")
MAPA_CSV = str(Path(BASE_DIR) / "diagnosticos" / "mapa_homologacion.csv")

# ============================================================
# -- LECTOR DE MAPA DE HOMOLOGACION -------------------------
# ============================================================

def load_mapa(mapa_path: str) -> Dict[str, List[str]]:
    """
    Lee mapa_homologacion.csv y devuelve un diccionario:
      { nombre_canonico: [alias1, alias2, ...] }
    donde los aliases son los nombres REALES encontrados en los DBF
    de la serie tradicional (columna alias_trad_encontrado).

    Si el archivo no existe, usa el mapa por defecto hardcodeado.
    """
    if not os.path.exists(mapa_path):
        print("  [WARN] mapa_homologacion.csv no encontrado en:")
        print("    " + mapa_path)
        print("  Usando mapa por defecto. Corre 02_scan_dbf.py primero para mejor cobertura.")
        return _mapa_defecto()

    try:
        df = pd.read_csv(mapa_path, encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(mapa_path, encoding="latin-1")

    mapa = {}
    for _, row in df.iterrows():
        canonico = str(row.get("nombre_canonico", "")).strip()
        encontrado = str(row.get("alias_trad_encontrado", "")).strip()
        if not canonico or encontrado in ("", "---", "nan"):
            continue
        aliases = [a.strip() for a in encontrado.split("/") if a.strip()]
        if aliases:
            mapa[canonico] = aliases

    print("  Mapa de homologacion cargado: " + str(len(mapa)) + " variables canonicas.")
    return mapa


def _mapa_defecto() -> Dict[str, List[str]]:
    """
    Mapa hardcodeado de respaldo (por si no corre 02 primero).
    Refleja los hallazgos del scan de enero 2025.
    """
    return {
        "FAC_P":    ["FAC", "FAC_MEN"],
        "FAC_TRI":  ["FAC"],
        "ENT":      ["ENT"],
        "N_REN":    ["N_REN", "RENGLON", "NUMREN", "NUM_REN", "NREN"],
        "T_LOC":    ["T_LOC", "TLOC", "TAM_LOC"],
        "EST_D":    ["EST_D"],
        # HALLAZGO: los nombres de documentacion no coinciden con DBF
        "NIV_INS":  ["NIV_INS"],         # doc dice NIVELAPROB
        "PAR_C":    ["PAR_C"],           # doc dice PARENT/PARENTESCO
        "BUSCAR5C": ["BUSCAR5C"],        # doc dice BUSCAR
        "DISPO":    ["DISPO"],           # doc dice DISPON
        "NODISPO":  ["NODISPO"],
    }


def find_alias_in_columns(columns: List[str], aliases: List[str]) -> Optional[str]:
    """
    Devuelve el primer alias de la lista que exista en columns (case-insensitive).
    """
    col_upper = {c.upper(): c for c in columns}
    for alias in aliases:
        if alias.upper() in col_upper:
            return col_upper[alias.upper()]
    return None


# ============================================================
# -- LECTOR DBF (sin dependencias externas) ------------------
# ============================================================

def read_dbf(path: Path, encoding: str = "latin-1") -> pd.DataFrame:
    """Lee un DBF clasico (dBASE III/IV) a DataFrame."""
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

    col_names = [f[0] for f in fields]
    return pd.DataFrame(rows, columns=col_names)


# ============================================================
# -- BUSQUEDA DE DBF POR MODULO ------------------------------
# ============================================================

# Nombre de sufijo numerico para la tradicional: tri + 2 digitos del anno
# Ej: 2005T1 -> suf = "105" (1 + 05)
def periodo_suf(year: int, quarter: int) -> str:
    return str(quarter) + str(year % 100).zfill(2)


# Indice global de DBFs: (modulo_canonico, suf) -> path
def build_index(rawroot: Path) -> Dict[Tuple[str, str], Path]:
    """
    Indexa todos los DBFs bajo rawroot con patron MOD<suf>.DBF.
    Acepta nombres con o sin prefijo ENOE_ y con variante COE1T/COE2T.
    """
    idx: Dict[Tuple[str, str], Path] = {}
    rx = re.compile(r"(?:ENOE_)?([A-Z0-9]+?)(\d{3})\.DBF$", re.IGNORECASE)
    for p in list(rawroot.rglob("*.dbf")) + list(rawroot.rglob("*.DBF")):
        m = rx.search(p.name.upper())
        if not m:
            continue
        mod = m.group(1).upper()
        suf = m.group(2)
        # Normaliza COE1T/COE2T -> COE1/COE2
        mod = re.sub(r"^(COE[12])T$", r"\1", mod)
        idx[(mod, suf)] = p
    return idx


def pick_dbf(idx: Dict[Tuple[str, str], Path],
             modulo: str, suf: str) -> Optional[Path]:
    modulo = modulo.upper()
    # COE1 acepta COE1T o COE1
    if modulo in ("COE1", "COE2"):
        for variant in (modulo + "T", modulo):
            if (variant, suf) in idx:
                return idx[(variant, suf)]
        return None
    return idx.get((modulo, suf))


# ============================================================
# -- CONSTRUCCION DE IDs -------------------------------------
# ============================================================

_ROSTER_SEARCH = ["N_REN", "RENGLON", "NUMREN", "NUM_REN", "NREN", "R_REN", "ID_ROSTER"]

HOG_KEY_CANDIDATES = ["CD_A", "ENT", "CON", "UPM", "V_SEL", "N_HOG", "H_MUD"]


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
# -- HOMOLOGACION DE VARIABLES CANONICAS ---------------------
# ============================================================

def homologate(df: pd.DataFrame, mapa: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Usando el mapa de homologacion, crea columnas canonicas
    a partir del alias que exista en el DataFrame.

    Regla: si el nombre canonico ya existe como columna, no hace nada.
    Si encuentra un alias, crea la columna canonica con ese valor.
    Si no encuentra ninguno, crea la columna canonica con NaN
    (para que el parquet tenga siempre el mismo schema).
    """
    cols = list(df.columns)
    for canonico, aliases in mapa.items():
        if canonico in df.columns:
            continue   # ya tiene el nombre correcto
        found = find_alias_in_columns(cols, aliases)
        if found:
            df[canonico] = df[found]
        # Si no se encuentra ninguno, NO crear columna vacia:
        # el parquet solo tendra las columnas que existen en ese trimestre.
        # El 04_scan_parquets (paso siguiente) reportara la ausencia.
    return df


# ============================================================
# -- CONSTRUCTOR DE UN TRIMESTRE -----------------------------
# ============================================================

def build_quarter(year: int, quarter: int,
                  idx: Dict[Tuple[str, str], Path],
                  outdir: Path,
                  mapa: Dict[str, List[str]],
                  encoding: str) -> bool:

    label   = str(year) + "T" + str(quarter)
    suf     = periodo_suf(year, quarter)
    out_pq  = outdir / ("enoe_trad_" + str(year) + "_T" + str(quarter) + ".parquet")
    out_csv = outdir / ("enoe_trad_" + str(year) + "_T" + str(quarter) + ".csv.gz")

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

    # -- Llaves del hogar ------------------------------------
    # Usa las que existen en SDEMT (orden canonico)
    keys_hog = [k for k in HOG_KEY_CANDIDATES if k in df_s.columns]
    if not keys_hog or "N_HOG" not in keys_hog:
        print("  [ERROR] No se encontraron llaves de hogar en SDEMT.")
        print("    Cols disponibles: " + str(list(df_s.columns[:20])))
        return False

    print("  Llaves hogar: " + str(keys_hog))

    # -- Construir IDs en SDEMT ------------------------------
    df_s["ID_HOGAR"]  = build_hog_id(df_s, keys_hog)
    df_s["ID_ROSTER"] = build_id_roster(df_s)

    if df_s["ID_ROSTER"].isna().any():
        n_miss = int(df_s["ID_ROSTER"].isna().sum())
        found_roster = [c for c in _ROSTER_SEARCH if c in df_s.columns]
        print("  [WARN] ID_ROSTER: " + str(n_miss) + " NAs. Cols disponibles: " + str(found_roster))

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
        print("  [WARN] Merge HOGT cambio nro de filas: " + str(n_before) + " -> " + str(len(df)))
    print("  Merge HOGT OK: " + str(len(keys_hog_hog)) + " llaves, " + str(len(df_h2)) + " hogares")

    # -- Merge VIVT (llaves de vivienda, sin N_HOG) ----------
    if not df_v.empty:
        viv_key_candidates = ["CD_A", "ENT", "CON", "UPM", "V_SEL", "N_PRO_VIV"]
        keys_viv = [k for k in viv_key_candidates if k in df.columns and k in df_v.columns]
        if keys_viv:
            df_v2 = dedup_on(df_v, keys_viv)
            n_before = len(df)
            df = df.merge(df_v2, on=keys_viv, how="left", suffixes=("", "_VIV"))
            if len(df) != n_before:
                print("  [WARN] Merge VIVT cambio nro de filas: " + str(n_before) + " -> " + str(len(df)))
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
        print("  [WARN] Duplicados (ID_HOGAR, ID_ROSTER): " + str(dup_id) + " <- revisar")
    else:
        print("  Unicidad ID_PERSONA: OK")

    # Reporte de variables canonicas disponibles
    canon_check = ["ENT", "FAC_P", "NIV_INS", "ANIOS_ESC", "PAR_C",
                   "EDA", "SEX", "HRSOCUP", "INGOCUP", "CLASE1",
                   "BUSCAR5C", "DISPO", "RAMA", "ZONA"]
    present = [v for v in canon_check if v in df.columns]
    absent  = [v for v in canon_check if v not in df.columns]
    print("  Vars canonicas OK : " + str(present))
    if absent:
        print("  Vars canonicas --- : " + str(absent))

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
    print("  03T_build_trad.py")
    print("  Constructor ENOE Tradicional  DBF -> Parquet")
    print("=" * 60)
    print("  BASE_DIR  : " + BASE_DIR)
    print("  RAW_DIR   : " + RAW_DIR)
    print("  OUT_DIR   : " + OUT_DIR)
    print("  MAPA_CSV  : " + MAPA_CSV)
    print("  Rango     : " + str(START_YEAR) + "T1 - " + str(END_YEAR) + "T" + str(END_QUARTER))
    print("  FORCE     : " + str(FORCE_REBUILD))

    rawroot = Path(RAW_DIR)
    outdir  = Path(OUT_DIR)

    if not rawroot.exists():
        print("")
        print("  [ERROR] RAW_DIR no existe: " + str(rawroot))
        print("  Verifica BASE_DIR y que hayas corrido 01T primero.")
        return

    # -- Cargar mapa de homologacion -------------------------
    print("")
    print("[1/3] Cargando mapa de homologacion...")
    mapa = load_mapa(MAPA_CSV)

    # -- Indexar todos los DBFs -----------------------------
    print("")
    print("[2/3] Indexando DBFs en " + RAW_DIR + " ...")
    idx = build_index(rawroot)
    print("  " + str(len(idx)) + " DBFs indexados con patron modulo+sufijo.")

    if not idx:
        print("  [ERROR] No se encontraron DBFs. Verifica que RAW_DIR tenga datos.")
        return

    # -- Procesar trimestres ---------------------------------
    print("")
    print("[3/3] Construyendo parquets...")

    ok = skip = fail = 0

    for year in range(START_YEAR, END_YEAR + 1):
        for quarter in range(1, 5):
            if year == END_YEAR and quarter > END_QUARTER:
                continue

            # Verifica rapidamente si el trimestre tiene DBFs antes de intentar
            suf = periodo_suf(year, quarter)
            sdem_exists = pick_dbf(idx, "SDEMT", suf)
            hogt_exists = pick_dbf(idx, "HOGT",  suf)

            if not sdem_exists and not hogt_exists:
                # No hay datos para este trimestre, silenciosamente omitir
                continue

            # Detectar si ya existe (para el contador correcto)
            out_pq  = outdir / ("enoe_trad_" + str(year) + "_T" + str(quarter) + ".parquet")
            out_csv = outdir / ("enoe_trad_" + str(year) + "_T" + str(quarter) + ".csv.gz")
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

    # -- Resumen final ---------------------------------------
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
        print("  [ATENCION] Revisa los mensajes [ERROR] y [WARN] arriba.")
    print("=" * 60)


if __name__ == "__main__":
    main()
