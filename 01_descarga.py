#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_descarga.py
==============
Descarga todos los insumos crudos del proyecto ENOE en una sola barrida:

  [A] Microdatos tradicional   DBF ZIPs  2005T1 – 2020T1
  [B] Microdatos nueva ENOE    DBF ZIPs  2023T1 – 2025T4
  [C] Validadores estratégicos XLS ZIPs  tradicional (anuales) + nueva (trimestrales)

Skip logic:
  - DBF      : skip si la carpeta año/Tq/ ya tiene 5 archivos .dbf
  - Validador: skip si el ZIP ya existe en validadores_2/raw_zip/

Primera corrida: descarga todo.
Corridas posteriores: solo descarga lo que falte.

ENOEN (2020T3–2022T4): excluida por quiebre metodológico COVID-19.

Estructura creada:
  ENOE/
  ├── tradicional/          ← DBFs micro (no se toca si ya existe)
  ├── nueva/                ← DBFs micro (no se toca si ya existe)
  └── validadores_2/        ← Todo limpio, reorganizado
      ├── raw_zip/
      │   ├── trad/         ← un ZIP por año (contiene 4 trim adentro)
      │   └── nueva/        ← un ZIP por año/trimestre
      └── extract/
          ├── trad/         ← XLS extraídos por año
          └── nueva/        ← XLS extraídos por año/trimestre
"""

import os
import time
import zipfile
from io import BytesIO
from pathlib import Path

import requests

# ============================================================
# CONFIG — ajusta solo BASE_DIR si cambias de máquina
# ============================================================
BASE_DIR = Path(r"C:\Users\vicou\OneDrive\Documentos\00_RESPALDO_VS\00_Labor_Market\ENOE")

TRAD_DIR  = BASE_DIR / "tradicional"
NUEVA_DIR = BASE_DIR / "nueva"

VALID_DIR     = BASE_DIR / "validadores_2"
RAW_TRAD_DIR  = VALID_DIR / "raw_zip" / "trad"
RAW_NUEVA_DIR = VALID_DIR / "raw_zip" / "nueva"
EXT_TRAD_DIR  = VALID_DIR / "extract" / "trad"
EXT_NUEVA_DIR = VALID_DIR / "extract" / "nueva"

TRAD_YEARS  = list(range(2005, 2021))
NUEVA_YEARS = list(range(2023, 2026))
NUEVA_END   = (2025, 4)   # último trimestre disponible

BASE_MICRO = "https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/microdatos"
BASE_VALID = "https://www.inegi.org.mx/contenidos/programas/enoe/15ymas/tabulados"


# ============================================================
# HELPERS
# ============================================================

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def download_zip(url: str, dest: Path, label: str,
                 timeout: int = 60, tries: int = 3) -> bool:
    """
    Descarga un ZIP a dest.
    Devuelve True si OK (incluyendo si ya existía y tiene tamaño razonable).
    """
    ensure_dir(dest.parent)

    if dest.exists() and dest.stat().st_size > 50_000:
        print(f"  ✔ Ya existe : {dest.name}")
        return True

    headers = {"User-Agent": "Mozilla/5.0 (ENOE-pipeline)"}

    for intento in range(1, tries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)

            if r.status_code == 404:
                return False   # silencioso — se probará siguiente candidato
            if r.status_code != 200:
                print(f"  ⚠ Status {r.status_code}: {label} (intento {intento})")
                time.sleep(intento)
                continue

            try:
                zipfile.ZipFile(BytesIO(r.content))
            except zipfile.BadZipFile:
                time.sleep(intento)
                continue

            dest.write_bytes(r.content)
            print(f"  ✔ Descargado: {dest.name}")
            return True

        except Exception as e:
            print(f"  ⚠ Error ({e}): {label} (intento {intento})")
            time.sleep(intento)

    return False


def extract_zip(zip_path: Path, out_dir: Path) -> bool:
    """Extrae si el directorio destino no tiene XLS todavía."""
    ensure_dir(out_dir)
    existing = list(out_dir.rglob("*.xls")) + list(out_dir.rglob("*.xlsx"))
    if existing:
        return True
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir)
        return True
    except Exception as e:
        print(f"  ⚠ No se pudo extraer {zip_path.name}: {e}")
        return False


def dbf_folder_ok(folder: Path) -> bool:
    """True si la carpeta tiene al menos 5 archivos .dbf."""
    if not folder.exists():
        return False
    dbfs = [f for f in folder.iterdir() if f.suffix.lower() == ".dbf"]
    return len(dbfs) >= 5


# ============================================================
# [A] MICRODATOS TRADICIONAL  2005T1 – 2020T1
# ============================================================

def descarga_trad():
    print("\n" + "=" * 60)
    print("  [A] MICRODATOS TRADICIONAL  (2005T1 – 2020T1)")
    print("=" * 60)

    ok = skip = fail = 0

    for año in TRAD_YEARS:
        trims = range(1, 2) if año == 2020 else range(1, 5)

        for t in trims:
            folder = TRAD_DIR / str(año) / f"T{t}"
            label  = f"Tradicional {año}T{t}"

            if dbf_folder_ok(folder):
                print(f"  ✔ Ya existe : {label}")
                skip += 1
                continue

            ensure_dir(folder)
            url  = f"{BASE_MICRO}/{año}trim{t}_dbf.zip"
            dest = folder / f"_{año}T{t}_tmp.zip"

            if download_zip(url, dest, label):
                try:
                    with zipfile.ZipFile(dest, "r") as zf:
                        zf.extractall(folder)
                    dest.unlink()
                    ok += 1
                    print(f"  ✔ Extraído : {label}")
                except Exception as e:
                    print(f"  ✖ Error extrayendo {label}: {e}")
                    fail += 1
            else:
                print(f"  ✖ No disponible: {label}")
                fail += 1

    print(f"\n  Resumen trad — OK: {ok}  Skip (ya existía): {skip}  Fail: {fail}")


# ============================================================
# [B] MICRODATOS NUEVA ENOE  2023T1 – 2025T4
# ============================================================

def descarga_nueva():
    print("\n" + "=" * 60)
    print("  [B] MICRODATOS NUEVA ENOE  (2023T1 – 2025T4)")
    print("=" * 60)
    print("  NOTA: ENOEN (2020T3–2022T4) excluida — quiebre metodológico COVID-19.\n")

    ok = skip = fail = 0

    for año in NUEVA_YEARS:
        for t in range(1, 5):
            if año == NUEVA_END[0] and t > NUEVA_END[1]:
                continue

            folder = NUEVA_DIR / str(año) / f"T{t}"
            label  = f"Nueva {año}T{t}"

            if dbf_folder_ok(folder):
                print(f"  ✔ Ya existe : {label}")
                skip += 1
                continue

            ensure_dir(folder)
            url  = f"{BASE_MICRO}/enoe_{año}_trim{t}_dbf.zip"
            dest = folder / f"_{año}T{t}_tmp.zip"

            if download_zip(url, dest, label):
                try:
                    with zipfile.ZipFile(dest, "r") as zf:
                        zf.extractall(folder)
                    dest.unlink()
                    ok += 1
                    print(f"  ✔ Extraído : {label}")
                except Exception as e:
                    print(f"  ✖ Error extrayendo {label}: {e}")
                    fail += 1
            else:
                print(f"  ✖ No disponible: {label}")
                fail += 1

    print(f"\n  Resumen nueva — OK: {ok}  Skip (ya existía): {skip}  Fail: {fail}")


# ============================================================
# [C] VALIDADORES ESTRATÉGICOS
#
#  Tradicional : ZIP anual → una hoja por trimestre adentro del XLS
#  Nueva       : ZIP por trimestre
#
#  Todo va a validadores_2/ (carpeta fresca, no mezcla con lo anterior)
# ============================================================

def _urls_trad(año: int):
    """INEGI usa diferentes nombres según el año — probamos hasta 3."""
    return [
        f"{BASE_VALID}/enoe_estrategicos_{año}_xls.zip",
        f"{BASE_VALID}/enoe_indicadores_estrategicos_{año}_xls.zip",
        f"{BASE_VALID}/enoe_n_indicadores_estrategicos_{año}_xls.zip",
    ]

def _urls_nueva(año: int, t: int):
    return [
        f"{BASE_VALID}/enoe_indicadores_estrategicos_{año}_trim{t}_xls.zip",
        f"{BASE_VALID}/enoe_n_indicadores_estrategicos_{año}_trim{t}_xls.zip",
    ]


def descarga_validadores():
    print("\n" + "=" * 60)
    print("  [C] VALIDADORES ESTRATÉGICOS  →  validadores_2/")
    print("=" * 60)

    ok_t = skip_t = fail_t = 0
    ok_n = skip_n = fail_n = 0

    # ── C.1 Tradicional (anuales) ─────────────────────────────
    print("\n  [C.1] Tradicional — un ZIP por año (2005–2020):")
    for año in TRAD_YEARS:
        dest = RAW_TRAD_DIR / str(año) / f"enoe_estrategicos_{año}_xls.zip"

        if dest.exists() and dest.stat().st_size > 50_000:
            print(f"  ✔ Ya existe : {dest.name}")
            skip_t += 1
            continue

        descargado = False
        for url in _urls_trad(año):
            if download_zip(url, dest, f"Validador trad {año}"):
                descargado = True
                break

        if descargado:
            extract_zip(dest, EXT_TRAD_DIR / str(año))
            ok_t += 1
        else:
            print(f"  ✖ No disponible: Validador trad {año}")
            fail_t += 1

    # ── C.2 Nueva (trimestrales) ──────────────────────────────
    print("\n  [C.2] Nueva ENOE — un ZIP por trimestre (2023T1–2025T4):")
    for año in NUEVA_YEARS:
        for t in range(1, 5):
            if año == NUEVA_END[0] and t > NUEVA_END[1]:
                continue

            dest = RAW_NUEVA_DIR / str(año) / f"enoe_indicadores_estrategicos_{año}_trim{t}_xls.zip"

            if dest.exists() and dest.stat().st_size > 50_000:
                print(f"  ✔ Ya existe : {dest.name}")
                skip_n += 1
                continue

            descargado = False
            for url in _urls_nueva(año, t):
                if download_zip(url, dest, f"Validador nueva {año}T{t}"):
                    descargado = True
                    break

            if descargado:
                extract_zip(dest, EXT_NUEVA_DIR / str(año) / f"T{t}")
                ok_n += 1
            else:
                print(f"  ✖ No disponible: Validador nueva {año}T{t}")
                fail_n += 1

    print(f"\n  Resumen validadores trad  — OK: {ok_t}  Skip: {skip_t}  Fail: {fail_t}")
    print(f"  Resumen validadores nueva — OK: {ok_n}  Skip: {skip_n}  Fail: {fail_n}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("  01_descarga.py — Pipeline ENOE")
    print("=" * 60)
    print(f"  BASE_DIR  : {BASE_DIR}")
    print(f"  TRAD_DIR  : {TRAD_DIR}")
    print(f"  NUEVA_DIR : {NUEVA_DIR}")
    print(f"  VALID_DIR : {VALID_DIR}  ← validadores_2/ (fresco)")

    descarga_trad()
    descarga_nueva()
    descarga_validadores()

    print("\n" + "=" * 60)
    print("  DESCARGA COMPLETA")
    print("=" * 60)
    print(f"  Microdatos trad : {TRAD_DIR}")
    print(f"  Microdatos nueva: {NUEVA_DIR}")
    print(f"  Validadores     : {VALID_DIR}")
    print("\n  Siguiente paso  : 02_scan_dbf.py")


if __name__ == "__main__":
    main()
