#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_scan_dbf.py
==============
Escanea los HEADERS de todos los DBF de ambas series ENOE
(tradicional 2005T1-2020T1 y nueva 2023T1-presente) SIN cargar datos.

Proposito
---------
Detectar que variables existen en cada modulo y trimestre, con que
nombre exacto las publica el INEGI, y cuando cambiaron de nombre.
Esto permite que los constructores 03T y 03N homologuen sin leer PDFs.

Outputs (en diagnosticos/)
--------------------------
  esquema_completo.csv    -> una fila por (serie, periodo, modulo, variable)
  mapa_homologacion.csv   -> nombre_canonico | alias_trad | alias_nueva | semaforo
  scan_dbf_reporte.html   -> tabla pivote legible en navegador

NOTA IMPORTANTE: EST vs ENT
  EST = Estrato de diseno muestral (variable de muestra, NO entidad)
  ENT = Entidad federativa (clave geografica 01-32)
  Son variables DISTINTAS. No confundir ni tratar como alias.

Como correr desde Spyder
------------------------
  1. Cambia BASE_DIR (unica linea a editar).
  2. Presiona F5.

Requisitos: pandas. Sin dependencias externas (no requiere dbf_read_lib).
"""

import os
import re
import struct
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

warnings.filterwarnings("ignore")

# ============================================================
# -- CONFIGURACION --- UNICA SECCION A EDITAR ----------------
# ============================================================

BASE_DIR = r"C:\Users\vicou\OneDrive\Documentos\00_RESPALDO_VS\00_Labor_Market\ENOE"

# Rangos de cada serie
TRAD_RANGE  = dict(start=2005, end=2020, end_q=1)   # 2005T1 -> 2020T1
NUEVA_RANGE = dict(start=2023, end=2025, end_q=4)   # 2023T1 -> 2025T4

# Si True -> re-escanea aunque ya existan los CSVs
FORCE_RESCAN = False

# -- Rutas derivadas (no editar) -----------------------------
TRAD_RAW  = str(Path(BASE_DIR) / "tradicional")
NUEVA_RAW = str(Path(BASE_DIR) / "nueva")
OUT_DIR   = str(Path(BASE_DIR) / "diagnosticos")

# ============================================================
# -- CATALOGO DE HOMOLOGACION (fuente: documentacion INEGI) --
# ============================================================
#
# Cada entrada: nombre_canonico -> dict con:
#   modulo      : modulo principal donde vive la variable
#   descripcion : que es (en ASCII)
#   alias_trad  : nombres usados en ENOE tradicional
#   alias_nueva : nombres usados en ENOE nueva
#   notas       : cambios documentados
#
# REGLA: EST != ENT
#   EST = Estrato de diseno muestral
#   ENT = Entidad federativa (01-32)

CATALOGO = {
    # -- Llaves de vivienda -----------------------------------
    "CD_A": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Ciudad autorrepresentada",
        "alias_trad":  ["CD_A"],
        "alias_nueva": ["CD_A"],
        "notas": "Estable en ambas series",
    },
    "ENT": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Entidad federativa (01-32). NO confundir con EST=Estrato",
        "alias_trad":  ["ENT"],
        "alias_nueva": ["ENT"],
        "notas": "Estable. EST es Estrato (diseno muestral), variable distinta.",
    },
    "CON": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Control de UPM",
        "alias_trad":  ["CON"],
        "alias_nueva": ["CON"],
        "notas": "Estable",
    },
    "UPM": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Unidad primaria de muestreo",
        "alias_trad":  ["UPM"],
        "alias_nueva": ["UPM"],
        "notas": "Estable",
    },
    "V_SEL": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Vivienda seleccionada",
        "alias_trad":  ["V_SEL"],
        "alias_nueva": ["V_SEL"],
        "notas": "Estable",
    },
    "N_PRO_VIV": {
        "modulo": "VIVT/HOGT",
        "descripcion": "Numero progresivo de vivienda en el listado",
        "alias_trad":  ["N_PRO_VIV"],
        "alias_nueva": ["N_PRO_VIV"],
        "notas": "Parte de llave longitudinal en nueva ENOE",
    },
    # -- Llaves de hogar --------------------------------------
    "N_HOG": {
        "modulo": "HOGT/SDEMT",
        "descripcion": "Numero de hogar en la vivienda",
        "alias_trad":  ["N_HOG"],
        "alias_nueva": ["N_HOG"],
        "notas": "Estable",
    },
    "H_MUD": {
        "modulo": "HOGT/SDEMT",
        "descripcion": "Hogar mudado (veces, 0-4). NO es ID de persona.",
        "alias_trad":  ["H_MUD"],
        "alias_nueva": ["H_MUD"],
        "notas": "Variable de seguimiento longitudinal. No usarla como ID persona.",
    },
    # -- Llave de persona (renglon) ---------------------------
    "N_REN": {
        "modulo": "SDEMT",
        "descripcion": "Numero de renglon (persona dentro del hogar)",
        "alias_trad":  ["N_REN", "RENGLON", "NUMREN", "NUM_REN", "NREN"],
        "alias_nueva": ["N_REN", "RENGLON", "NUMREN"],
        "notas": "El nombre exacto varia por trimestre. Buscar el primero que exista.",
    },
    # -- Ola de rotacion --------------------------------------
    "N_ENT": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Numero de entrevista/visita (ola 1-5). NO entra a llave longitudinal.",
        "alias_trad":  ["N_ENT"],
        "alias_nueva": ["N_ENT"],
        "notas": "Identifica la ola de rotacion. No usar como llave de merge entre periodos.",
    },
    "R_DEF": {
        "modulo": "HOGT",
        "descripcion": "Resultado definitivo de entrevista. 00=completa.",
        "alias_trad":  ["R_DEF"],
        "alias_nueva": ["R_DEF"],
        "notas": "Filtro estandar: R_DEF='00'",
    },
    "PER": {
        "modulo": "VIVT/HOGT/SDEMT",
        "descripcion": "Periodo (trimestre+anno). NO usar como llave longitudinal.",
        "alias_trad":  ["PER"],
        "alias_nueva": ["PER"],
        "notas": "Cambia cada trimestre. No usar en merge t->t+1.",
    },
    # -- Factores de expansion --------------------------------
    "FAC_P": {
        "modulo": "SDEMT/VIVT",
        "descripcion": "Factor de expansion persona (nombre canonico del pipeline)",
        "alias_trad":  ["FAC", "FAC_MEN"],
        "alias_nueva": ["FAC_TRI", "FAC_MEN"],
        "notas": "CAMBIO CLAVE: tradicional usa FAC; nueva usa FAC_TRI.",
    },
    "FAC_TRI": {
        "modulo": "VIVT/SDEMT",
        "descripcion": "Ponderador trimestral (nombre original INEGI en nueva ENOE)",
        "alias_trad":  ["FAC"],
        "alias_nueva": ["FAC_TRI"],
        "notas": "En tradicional se llamaba FAC. En nueva es FAC_TRI.",
    },
    # -- Diseno muestral (NO confundir con ENT) ---------------
    "EST": {
        "modulo": "VIVT/HOGT",
        "descripcion": "Estrato de diseno muestral. Variable de muestra, NO entidad.",
        "alias_trad":  ["EST"],
        "alias_nueva": ["EST"],
        "notas": "EST=Estrato. DISTINTO de ENT=Entidad. Error comun confundirlos.",
    },
    "EST_D_TRI": {
        "modulo": "VIVT/HOGT",
        "descripcion": "Estrato de diseno trimestral",
        "alias_trad":  ["EST_D_TRI"],
        "alias_nueva": ["EST_D_TRI"],
        "notas": "Variable de diseno muestral",
    },
    # -- Geografia y tamano de localidad ----------------------
    "T_LOC": {
        "modulo": "VIVT/SDEMT",
        "descripcion": "Tamano de localidad",
        "alias_trad":  ["T_LOC", "TLOC", "TAM_LOC"],
        "alias_nueva": ["T_LOC_TRI", "T_LOC_MEN", "T_LOC"],
        "notas": "CAMBIO: trad usa T_LOC; nueva tiene T_LOC_TRI y T_LOC_MEN separados",
    },
    "MUN": {
        "modulo": "VIVT/HOGT",
        "descripcion": "Municipio (puede omitirse por confidencialidad)",
        "alias_trad":  ["MUN"],
        "alias_nueva": ["MUN"],
        "notas": "",
    },
    "UR": {
        "modulo": "VIVT/SDEMT",
        "descripcion": "Urbano/rural (1=urbano, 2=complemento y rural)",
        "alias_trad":  ["UR"],
        "alias_nueva": ["UR"],
        "notas": "",
    },
    # -- Sociodemograficas (SDEMT) ----------------------------
    "EDA": {
        "modulo": "SDEMT",
        "descripcion": "Edad en anios cumplidos",
        "alias_trad":  ["EDA"],
        "alias_nueva": ["EDA"],
        "notas": "Estable",
    },
    "SEX": {
        "modulo": "SDEMT",
        "descripcion": "Sexo (1=hombre, 2=mujer)",
        "alias_trad":  ["SEX"],
        "alias_nueva": ["SEX"],
        "notas": "Estable",
    },
    # HALLAZGO scan ene-2025: PARENT/PARENTESCO no existen en los DBF.
    # El nombre real es PAR_C. De PAR_C==1 se deriva si la persona es jefe.
    "PAR_C": {
        "modulo": "SDEMT",
        "descripcion": "Parentesco con jefe del hogar (codigo). PAR_C==1 -> jefe.",
        "alias_trad":  ["PAR_C"],
        "alias_nueva": ["PAR_C"],
        "notas": "HALLAZGO: documentacion INEGI usa PARENT/PARENTESCO "
                 "pero el DBF real usa PAR_C. Descubierto en scan ene-2025. "
                 "JEFEH (sexo jefe) tampoco esta en DBF; derivar de PAR_C + SEX.",
    },
    "CS_P13_1": {
        "modulo": "SDEMT",
        "descripcion": "Nivel de instruccion (pregunta 13.1 cuestionario)",
        "alias_trad":  ["CS_P13_1"],
        "alias_nueva": ["CS_P13_1"],
        "notas": "Complementar con CS_P13_2 y NIV_INS para escolaridad completa",
    },
    # HALLAZGO scan ene-2025: NIVELAPROB no existe en DBF.
    # El nombre real es NIV_INS. CS_P13_1 es la pregunta raw, NIV_INS es precodificado.
    "NIV_INS": {
        "modulo": "SDEMT",
        "descripcion": "Nivel de instruccion precodificado (nombre real en DBF).",
        "alias_trad":  ["NIV_INS"],
        "alias_nueva": ["NIV_INS"],
        "notas": "HALLAZGO: documentacion usa NIVELAPROB pero DBF real usa NIV_INS. "
                 "Descubierto en scan ene-2025. Presente en ambas series, 100% trimestres.",
    },
    "ANIOS_ESC": {
        "modulo": "SDEMT",
        "descripcion": "Anios de escolaridad (variable precodificada)",
        "alias_trad":  ["ANIOS_ESC"],
        "alias_nueva": ["ANIOS_ESC"],
        "notas": "",
    },
    "C_RES": {
        "modulo": "SDEMT",
        "descripcion": "Condicion de residencia (1=residente habitual, 3=residente nuevo)",
        "alias_trad":  ["C_RES"],
        "alias_nueva": ["C_RES"],
        "notas": "Filtro estandar junto con R_DEF='00'",
    },
    # -- Condicion de actividad (SDEMT precodificadas) --------
    "CLASE1": {
        "modulo": "SDEMT",
        "descripcion": "Condicion de actividad (PEA/PNEA)",
        "alias_trad":  ["CLASE1"],
        "alias_nueva": ["CLASE1"],
        "notas": "",
    },
    "CLASE2": {
        "modulo": "SDEMT",
        "descripcion": "Subclasificacion de actividad",
        "alias_trad":  ["CLASE2"],
        "alias_nueva": ["CLASE2"],
        "notas": "",
    },
    "CLASE3": {
        "modulo": "SDEMT",
        "descripcion": "Subclasificacion adicional",
        "alias_trad":  ["CLASE3"],
        "alias_nueva": ["CLASE3"],
        "notas": "",
    },
    "POS_OCU": {
        "modulo": "SDEMT",
        "descripcion": "Posicion en la ocupacion",
        "alias_trad":  ["POS_OCU"],
        "alias_nueva": ["POS_OCU"],
        "notas": "",
    },
    "SUB_O": {
        "modulo": "SDEMT",
        "descripcion": "Condicion de subocupacion",
        "alias_trad":  ["SUB_O"],
        "alias_nueva": ["SUB_O"],
        "notas": "",
    },
    # -- Mercado laboral (SDEMT) ------------------------------
    "HRSOCUP": {
        "modulo": "SDEMT",
        "descripcion": "Horas trabajadas semana de referencia",
        "alias_trad":  ["HRSOCUP"],
        "alias_nueva": ["HRSOCUP"],
        "notas": "",
    },
    "INGOCUP": {
        "modulo": "SDEMT",
        "descripcion": "Ingreso por trabajo (pesos mensuales)",
        "alias_trad":  ["INGOCUP"],
        "alias_nueva": ["INGOCUP"],
        "notas": "",
    },
    "ING_X_HRS": {
        "modulo": "SDEMT",
        "descripcion": "Ingreso por hora trabajada",
        "alias_trad":  ["ING_X_HRS"],
        "alias_nueva": ["ING_X_HRS"],
        "notas": "",
    },
    "SALARIO": {
        "modulo": "SDEMT",
        "descripcion": "Salario minimo de zona asignado al registro",
        "alias_trad":  ["SALARIO"],
        "alias_nueva": ["SALARIO"],
        "notas": "",
    },
    "ZONA": {
        "modulo": "SDEMT",
        "descripcion": "Zona salarial (cambio en 2019: FRONTERA NORTE o GENERAL)",
        "alias_trad":  ["ZONA"],
        "alias_nueva": ["ZONA"],
        "notas": "3 zonas 2005-2012, 2 zonas 2012-2015, 1 zona 2015-2018, 2 zonas 2019+",
    },
    "RAMA": {
        "modulo": "SDEMT/COE1",
        "descripcion": "Rama de actividad economica",
        "alias_trad":  ["RAMA"],
        "alias_nueva": ["RAMA"],
        "notas": "",
    },
    "SCIAN": {
        "modulo": "COE1",
        "descripcion": "Clasificacion industrial SCIAN",
        "alias_trad":  ["SCIAN"],
        "alias_nueva": ["SCIAN"],
        "notas": "Version 2013->2018 actualizada en 2021",
    },
    "TUE1": {
        "modulo": "SDEMT/COE1",
        "descripcion": "Tipo de unidad economica (1er trabajo)",
        "alias_trad":  ["TUE1"],
        "alias_nueva": ["TUE1"],
        "notas": "",
    },
    "TUE2": {
        "modulo": "SDEMT/COE1",
        "descripcion": "Tipo de unidad economica (2do trabajo)",
        "alias_trad":  ["TUE2"],
        "alias_nueva": ["TUE2"],
        "notas": "",
    },
    # HALLAZGO scan ene-2025: T_PUE no existe en los DBF escaneados.
    # Es una variable derivada/precodificada que se construye desde preguntas de COE1.
    # Incluida en el catalogo como referencia pero marcada como derivada.
    "T_PUE_DERIVADA": {
        "modulo": "COE1",
        "descripcion": "Tipo de puesto ocupado. DERIVADA, no esta en DBF directamente.",
        "alias_trad":  [],
        "alias_nueva": [],
        "notas": "HALLAZGO scan ene-2025: T_PUE no aparece en ningun DBF. "
                 "Se construye desde preguntas P3 del COE1. No buscar como variable directa.",
    },
    # HALLAZGO scan ene-2025: BUSCAR no existe. El nombre real es BUSCAR5C.
    "BUSCAR5C": {
        "modulo": "SDEMT",
        "descripcion": "Busqueda de trabajo en 5 categorias (desocupados y PNEA disponible)",
        "alias_trad":  ["BUSCAR5C"],
        "alias_nueva": ["BUSCAR5C"],
        "notas": "HALLAZGO scan ene-2025: nombre en documentacion es BUSCAR "
                 "pero DBF real usa BUSCAR5C. Presente en ambas series.",
    },
    # HALLAZGO scan ene-2025: DISPON no existe. Los nombres reales son DISPO y NODISPO.
    "DISPO": {
        "modulo": "SDEMT",
        "descripcion": "Disponibilidad para trabajar (PNEA disponible). 1=disponible.",
        "alias_trad":  ["DISPO"],
        "alias_nueva": ["DISPO"],
        "notas": "HALLAZGO scan ene-2025: nombre en documentacion es DISPON "
                 "pero DBF usa DISPO. Complementar con NODISPO (razon de no disponibilidad).",
    },
    "NODISPO": {
        "modulo": "SDEMT",
        "descripcion": "Razon de no disponibilidad para trabajar (PNEA no disponible)",
        "alias_trad":  ["NODISPO"],
        "alias_nueva": ["NODISPO"],
        "notas": "Complementa a DISPO. Par de variables para caracterizar a la PNEA.",
    },
    # -- Hogar (HOGT) -----------------------------------------
    # HALLAZGO scan ene-2025: JEFEH no existe en los DBF.
    # El sexo del jefe se deriva cruzando PAR_C==1 con SEX en el SDEMT.
    # HALLAZGO scan ene-2025: NUMPERS no existe en los DBF.
    # El numero de personas por hogar se calcula como conteo de N_REN por ID_HOGAR.
    "N_HIJ": {
        "modulo": "SDEMT",
        "descripcion": "Numero de hijos nacidos vivos (mujeres)",
        "alias_trad":  ["N_HIJ"],
        "alias_nueva": ["N_HIJ"],
        "notas": "Util para analisis de participacion femenina y fecundidad",
    },
    "HIJ5C": {
        "modulo": "SDEMT",
        "descripcion": "Hijos en categorias de 5 grupos (precodificado)",
        "alias_trad":  ["HIJ5C"],
        "alias_nueva": ["HIJ5C"],
        "notas": "Version categorizada de N_HIJ",
    },
    # -- Variables nuevas solo en HOGT nueva (hallazgo scan) --
    # La nueva ENOE agrego claves geograficas enriquecidas en HOGT.
    "CVE_ENT": {
        "modulo": "HOGT/SDEMT",
        "descripcion": "Clave de entidad (formato texto, nueva ENOE)",
        "alias_trad":  [],
        "alias_nueva": ["CVE_ENT"],
        "notas": "HALLAZGO scan ene-2025: variable nueva solo en nueva ENOE. "
                 "Equivalente a ENT pero en formato clave geografica INEGI.",
    },
    "CVEGEO": {
        "modulo": "HOGT/SDEMT",
        "descripcion": "Clave geografica completa (estado+municipio+localidad)",
        "alias_trad":  [],
        "alias_nueva": ["CVEGEO"],
        "notas": "HALLAZGO scan ene-2025: variable nueva solo en nueva ENOE. "
                 "Permite cruzar con otros registros administrativos del INEGI.",
    },
}

# ============================================================
# -- LECTOR DE HEADERS DBF (sin cargar datos) ----------------
# ============================================================

def read_dbf_fields(path: Path) -> List[str]:
    """
    Lee solo los nombres de campos del header DBF.
    No carga ningun registro - es casi instantaneo.
    """
    try:
        with path.open("rb") as f:
            header = f.read(32)
            if len(header) < 32:
                return []
            f.seek(32)
            fields = []
            while True:
                desc = f.read(32)
                if not desc or desc[0] == 0x0D:
                    break
                raw  = desc[0:11].split(b"\x00", 1)[0]
                name = raw.decode("latin-1", errors="ignore").strip().upper()
                if name:
                    fields.append(name)
        return fields
    except Exception as e:
        print("    [WARN] No se pudo leer " + path.name + ": " + str(e))
        return []


# ============================================================
# -- DETECCION DE MODULO -------------------------------------
# ============================================================

MODULOS_CANON = ["SDEMT", "HOGT", "VIVT", "COE1", "COE2"]

def detect_modulo(filename: str) -> str:
    name = filename.upper()
    for mod in MODULOS_CANON:
        if re.search(r"(?:ENOE_)?" + re.escape(mod) + r"T?\d*\.DBF", name):
            return mod
    return "OTRO"


# ============================================================
# -- ITERADOR DE PERIODOS ------------------------------------
# ============================================================

def iter_periods(rawroot: Path, start: int, end: int, end_q: int):
    for y in range(start, end + 1):
        for q in range(1, 5):
            if y == end and q > end_q:
                continue
            folder = rawroot / str(y) / ("T" + str(q))
            if folder.is_dir():
                dbfs = list(folder.glob("*.dbf")) + list(folder.glob("*.DBF"))
                if dbfs:
                    yield y, q, folder


# ============================================================
# -- ESCANEO PRINCIPAL ---------------------------------------
# ============================================================

def scan_serie(serie: str, rawroot: Path, rng: dict) -> List[dict]:
    rows = []
    periods = list(iter_periods(rawroot, rng["start"], rng["end"], rng["end_q"]))

    if not periods:
        print("  [WARN] No se encontraron carpetas con DBF en: " + str(rawroot))
        return rows

    print("")
    print("  Serie: " + serie.upper() + "  (" + str(len(periods)) + " trimestres)")
    print("  " + "-" * 55)

    for y, q, folder in periods:
        periodo = str(y) + "T" + str(q)
        dbfs = sorted(list(folder.glob("*.dbf")) + list(folder.glob("*.DBF")))
        modulos_ok = []

        for dbf_path in dbfs:
            modulo = detect_modulo(dbf_path.name)
            if modulo == "OTRO":
                continue
            fields = read_dbf_fields(dbf_path)
            modulos_ok.append(modulo)
            for var in fields:
                rows.append({
                    "serie":    serie,
                    "periodo":  periodo,
                    "modulo":   modulo,
                    "archivo":  dbf_path.name,
                    "variable": var,
                })

        n_vars = sum(1 for r in rows if r["periodo"] == periodo
                     and r["serie"] == serie)
        print("  " + periodo + "  modulos: " + str(modulos_ok) +
              "  (" + str(n_vars) + " vars)")

    return rows


# ============================================================
# -- MAPA DE HOMOLOGACION ------------------------------------
# ============================================================

def build_mapa(df_esquema: pd.DataFrame) -> pd.DataFrame:
    vars_trad  = set(df_esquema.loc[df_esquema["serie"] == "tradicional", "variable"])
    vars_nueva = set(df_esquema.loc[df_esquema["serie"] == "nueva",       "variable"])

    rows = []
    for canonico, info in CATALOGO.items():
        found_trad  = [a for a in info["alias_trad"]  if a in vars_trad]
        found_nueva = [a for a in info["alias_nueva"] if a in vars_nueva]

        mods_trad = sorted(df_esquema.loc[
            (df_esquema["serie"] == "tradicional") &
            (df_esquema["variable"].isin(found_trad)), "modulo"].unique())
        mods_nueva = sorted(df_esquema.loc[
            (df_esquema["serie"] == "nueva") &
            (df_esquema["variable"].isin(found_nueva)), "modulo"].unique())

        if found_trad and found_nueva:
            if found_trad[0] == found_nueva[0]:
                semaforo = "OK - igual en ambas"
            else:
                semaforo = "REVISAR - nombre distinto"
        elif found_trad and not found_nueva:
            semaforo = "REVISAR - solo en tradicional"
        elif found_nueva and not found_trad:
            semaforo = "REVISAR - solo en nueva"
        else:
            semaforo = "NO ENCONTRADA"

        rows.append({
            "nombre_canonico":       canonico,
            "modulo_principal":      info["modulo"],
            "descripcion":           info["descripcion"],
            "alias_trad_esperado":   " / ".join(info["alias_trad"]),
            "alias_trad_encontrado": " / ".join(found_trad)  if found_trad  else "---",
            "modulos_trad":          " / ".join(mods_trad)   if mods_trad   else "---",
            "alias_nueva_esperado":  " / ".join(info["alias_nueva"]),
            "alias_nueva_encontrado":" / ".join(found_nueva) if found_nueva else "---",
            "modulos_nueva":         " / ".join(mods_nueva)  if mods_nueva  else "---",
            "semaforo":              semaforo,
            "notas":                 info["notas"],
        })

    df = pd.DataFrame(rows)
    # Ordenar: primero los que necesitan atencion
    order_map = {"OK": 0, "REVISAR": 1, "NO": 2}
    df["_ord"] = df["semaforo"].apply(
        lambda s: order_map.get(s.split(" ")[0], 3))
    return df.sort_values("_ord").drop(columns=["_ord"])


# ============================================================
# -- TABLA PIVOTE: variable x periodo ------------------------
# ============================================================

def build_pivot(df_esquema: pd.DataFrame,
                serie: str, modulo: str) -> pd.DataFrame:
    sub = df_esquema[
        (df_esquema["serie"]  == serie) &
        (df_esquema["modulo"] == modulo)
    ].copy()
    if sub.empty:
        return pd.DataFrame()

    periodos  = sorted(sub["periodo"].unique(),
                       key=lambda p: (int(p[:4]), int(p[-1])))
    variables = sorted(sub["variable"].unique())

    present = defaultdict(set)
    for _, row in sub.iterrows():
        present[row["variable"]].add(row["periodo"])

    data = {p: ["SI" if p in present[v] else "" for v in variables]
            for p in periodos}
    df = pd.DataFrame(data, index=variables)
    df.index.name = "variable"
    return df


# ============================================================
# -- GENERADOR HTML ------------------------------------------
# ============================================================

def _cell_style(val: str, is_semaforo: bool = False) -> str:
    if is_semaforo:
        if val.startswith("OK"):
            return "background:#d5f5e3;color:#1a5c34;"
        if val.startswith("REVISAR"):
            return "background:#fef9e7;color:#7d6608;"
        if val.startswith("NO"):
            return "background:#fadbd8;color:#922b21;"
        return ""
    if val == "SI":
        return "background:#d5f5e3;color:#1a5c34;text-align:center;"
    if val == "":
        return "background:#f8f8f8;color:#ccc;text-align:center;"
    return ""


def df_to_html_table(df: pd.DataFrame,
                     semaforo_col: Optional[str] = None,
                     compact: bool = False) -> str:
    if df.empty:
        return "<p><em>Sin datos</em></p>"
    fs  = "10px" if compact else "12px"
    pad = "2px 5px" if compact else "5px 10px"

    header_cells = ""
    if df.index.name:
        header_cells += "<th>" + df.index.name + "</th>"
    for col in df.columns:
        header_cells += "<th>" + str(col) + "</th>"

    body_rows = ""
    for idx, row in df.iterrows():
        cells = ""
        if df.index.name:
            cells += ("<td style='font-family:Consolas;padding:" +
                      pad + ";font-size:" + fs + ";'>" + str(idx) + "</td>")
        for col in df.columns:
            val   = str(row[col]) if row[col] is not None else ""
            is_s  = (col == semaforo_col)
            style = "padding:" + pad + ";font-size:" + fs + ";" + _cell_style(val, is_s)
            cells += "<td style='" + style + "'>" + val + "</td>"
        body_rows += "<tr>" + cells + "</tr>\n"

    return ("<table><thead><tr>" + header_cells + "</tr></thead>"
            "<tbody>" + body_rows + "</tbody></table>")


def build_html(df_mapa: pd.DataFrame, pivots: dict,
               n_trad: int, n_nueva: int, fecha: str = "") -> str:

    pivot_html = ""
    for (serie, modulo), df_piv in pivots.items():
        color = "#1540a8" if serie == "tradicional" else "#00a8c0"
        label = "TRADICIONAL" if serie == "tradicional" else "NUEVA ENOE"
        pivot_html += (
            "<h3 style='color:" + color + ";'>" + label +
            " &mdash; Modulo " + modulo + "</h3>"
            "<div style='overflow-x:auto;'>" +
            df_to_html_table(df_piv, compact=True) +
            "</div>"
        )

    return (
        "<!DOCTYPE html>\n"
        "<html lang='es'>\n"
        "<head>\n"
        "<meta charset='utf-8'>\n"
        "<title>Scan DBF ENOE</title>\n"
        "<style>\n"
        "body{font-family:Calibri,Arial,sans-serif;font-size:13px;"
        "background:#f0f5fb;color:#1b2a4a;margin:20px;}\n"
        "h1{color:#1540a8;border-bottom:3px solid #00a8c0;padding-bottom:6px;}\n"
        "h2{color:#1540a8;margin-top:28px;}\n"
        "h3{color:#1976d2;margin-top:18px;}\n"
        "table{border-collapse:collapse;margin-bottom:18px;}\n"
        "th{background:#1540a8;color:white;padding:5px 8px;"
        "font-size:11px;text-align:left;}\n"
        "td{border-bottom:1px solid #d9e4f0;}\n"
        "tr:nth-child(even){background:#edf2fa;}\n"
        ".box{background:white;border-left:4px solid #00a8c0;"
        "padding:10px 16px;margin:10px 0;border-radius:4px;}\n"
        ".warn{border-left-color:#e8a020;background:#fef9e7;"
        "padding:10px 14px;margin:10px 0;}\n"
        "footer{margin-top:30px;font-size:11px;color:#64748b;}\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        "<h1>Scan DBF ENOE &mdash; Mapa de Homologacion de Variables</h1>\n"
        "<div class='box'>"
        "<strong>ENOE Tradicional:</strong> " + str(n_trad) + " trimestres escaneados<br>"
        "<strong>Nueva ENOE:</strong> " + str(n_nueva) + " trimestres escaneados<br>"
        "<strong>Proposito:</strong> Este reporte es el insumo de los constructores 03T y 03N. "
        "Muestra que nombre exacto tiene cada variable en cada modulo y periodo."
        "</div>\n"
        "<div class='warn'>"
        "<strong>ADVERTENCIA: EST distinto de ENT</strong><br>"
        "EST = Estrato de diseno muestral (variable de muestra).<br>"
        "ENT = Entidad federativa (01-32).<br>"
        "Son variables DISTINTAS. No usar como alias."
        "</div>\n"
        "<h2>1. Mapa de Homologacion &mdash; Variables Canonicas</h2>\n"
        "<p>Cruza el catalogo de variables conocidas contra los DBF encontrados.</p>\n" +
        df_to_html_table(df_mapa, semaforo_col="semaforo") +
        "<h2>2. Pivotes de Presencia por Modulo y Periodo</h2>\n"
        "<p>SI = variable presente en ese trimestre. Vacio = ausente.</p>\n" +
        pivot_html +
        "<h2>3. Leyenda</h2>\n"
        "<table style='width:auto;'>"
        "<tr><td style='color:#1a5c34;font-weight:bold;padding:4px 8px;'>OK</td>"
        "<td>Nombre identico en tradicional y nueva. Sin cambios.</td></tr>"
        "<tr><td style='color:#7d6608;font-weight:bold;padding:4px 8px;'>REVISAR</td>"
        "<td>Nombre diferente entre series o ausente en una. El constructor "
        "debe buscar el alias correcto.</td></tr>"
        "<tr><td style='color:#922b21;font-weight:bold;padding:4px 8px;'>NO ENCONTRADA</td>"
        "<td>Variable del catalogo no encontrada en ningun DBF escaneado. "
        "Verificar nombre.</td></tr>"
        "</table>\n"
        "<footer>Generado por 02_scan_dbf.py &mdash; " + fecha + " &mdash; BASE_DIR: " + BASE_DIR + "</footer>\n"
        "</body></html>"
    )


# ============================================================
# -- MAIN ----------------------------------------------------
# ============================================================

def main() -> None:
    import datetime
    fecha_hoy = datetime.date.today().strftime("%Y-%m-%d")

    print("=" * 60)
    print("  02_scan_dbf.py  [" + fecha_hoy + "]")
    print("  Scan de Headers DBF --- ENOE Tradicional y Nueva")
    print("=" * 60)
    print("  BASE_DIR  : " + BASE_DIR)
    print("  TRAD_RAW  : " + TRAD_RAW)
    print("  NUEVA_RAW : " + NUEVA_RAW)
    print("  OUT_DIR   : " + OUT_DIR)

    os.makedirs(OUT_DIR, exist_ok=True)

    # Archivos con fecha para historial de versiones
    out_esquema    = os.path.join(OUT_DIR, "esquema_completo.csv")
    out_mapa       = os.path.join(OUT_DIR, "mapa_homologacion.csv")
    out_html_dated = os.path.join(OUT_DIR, "scan_dbf_reporte_" + fecha_hoy + ".html")
    out_html_last  = os.path.join(OUT_DIR, "scan_dbf_reporte_ultimo.html")

    # -- Skip si ya existe -----------------------------------
    if not FORCE_RESCAN and os.path.exists(out_mapa):
        print("")
        print("  Reporte ya existe: " + out_mapa)
        print("  Para regenerar: pon FORCE_RESCAN = True")
        print("=" * 60)
        return

    # -- 1. Escanear -----------------------------------------
    print("")
    print("[1/4] Escaneando ENOE Tradicional...")
    rows_trad = scan_serie("tradicional", Path(TRAD_RAW), TRAD_RANGE)

    print("")
    print("[2/4] Escaneando Nueva ENOE...")
    rows_nueva = scan_serie("nueva", Path(NUEVA_RAW), NUEVA_RANGE)

    if not rows_trad and not rows_nueva:
        print("")
        print("  [ERROR] No se encontraron DBF en ninguna de las rutas.")
        print("  Verifica que BASE_DIR apunte correctamente a:")
        print("    " + TRAD_RAW)
        print("    " + NUEVA_RAW)
        return

    df_esquema = pd.DataFrame(rows_trad + rows_nueva)

    # -- 2. Mapa de homologacion -----------------------------
    print("")
    print("[3/4] Construyendo mapa de homologacion...")
    df_mapa = build_mapa(df_esquema)

    # -- 3. Pivotes por modulo -------------------------------
    print("[4/4] Generando pivotes y reporte HTML...")
    pivots = {}
    for serie in ["tradicional", "nueva"]:
        for modulo in MODULOS_CANON:
            piv = build_pivot(df_esquema, serie, modulo)
            if not piv.empty:
                pivots[(serie, modulo)] = piv

    # -- 4. Exportar -----------------------------------------
    n_trad  = df_esquema[df_esquema["serie"] == "tradicional"]["periodo"].nunique()
    n_nueva = df_esquema[df_esquema["serie"] == "nueva"]["periodo"].nunique()

    df_esquema.to_csv(out_esquema, index=False, encoding="utf-8-sig")
    df_mapa.to_csv(out_mapa,       index=False, encoding="utf-8-sig")

    html = build_html(df_mapa, pivots, n_trad, n_nueva, fecha_hoy)
    # Guarda con fecha (historial) Y como "ultimo" (para referencia rapida)
    for out_path in [out_html_dated, out_html_last]:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(html)

    # -- 5. Resumen de hallazgos en consola ------------------
    print("")
    print("=" * 60)
    print("  HALLAZGOS: VARIABLES CON DIFERENCIAS ENTRE SERIES")
    print("=" * 60)
    diffs = df_mapa[~df_mapa["semaforo"].str.startswith("OK")]
    if diffs.empty:
        print("  Todas las variables canonicas tienen el mismo nombre.")
    else:
        print("  Estas diferencias deben documentarse en el paper (seccion datos):")
        print("")
        for _, row in diffs.iterrows():
            print("  [" + row["semaforo"][:20].ljust(20) + "]  " +
                  row["nombre_canonico"].ljust(18) +
                  "trad=" + str(row["alias_trad_encontrado"]).ljust(16) +
                  "nueva=" + str(row["alias_nueva_encontrado"]))

    # Hallazgos fijos documentados en el catalogo
    print("")
    print("=" * 60)
    print("  CORRECCIONES AL CATALOGO (nombres DBF reales vs documentacion)")
    print("=" * 60)
    hallazgos = [
        ("FAC -> FAC_TRI",
         "Factor de expansion cambio de nombre en nueva ENOE"),
        ("T_LOC -> T_LOC_TRI + T_LOC_MEN",
         "Se dividio en dos variables en nueva ENOE"),
        ("EST_D -> EST_D_TRI",
         "Estrato de diseno renombrado en nueva ENOE"),
        ("NIVELAPROB -> NIV_INS",
         "Nombre real en DBF. Documentacion INEGI usa NIVELAPROB."),
        ("PARENT/PARENTESCO -> PAR_C",
         "Nombre real en DBF. Documentacion usa PARENT."),
        ("BUSCAR -> BUSCAR5C",
         "Nombre real en DBF. Documentacion usa BUSCAR."),
        ("DISPON -> DISPO + NODISPO",
         "Nombre real en DBF. Dos variables, no una."),
        ("T_PUE: NO ESTA EN DBF",
         "Variable derivada desde preguntas P3 del COE1."),
        ("JEFEH: NO ESTA EN DBF",
         "Derivar de PAR_C==1 cruzado con SEX."),
        ("NUMPERS: NO ESTA EN DBF",
         "Calcular como conteo de personas por ID_HOGAR."),
        ("CVEGEO / CVE_ENT: solo en nueva ENOE",
         "Claves geograficas enriquecidas agregadas en 2023."),
    ]
    for correccion, nota in hallazgos:
        print("  " + correccion.ljust(32) + "  " + nota)

    print("")
    print("=" * 60)
    print("  ARCHIVOS GENERADOS")
    print("=" * 60)
    print("  Esquema completo      : " + out_esquema)
    print("  Mapa homologacion     : " + out_mapa)
    print("  Reporte HTML (fecha)  : " + out_html_dated)
    print("  Reporte HTML (ultimo) : " + out_html_last)
    print("")
    print("  -> El HTML con fecha queda como registro historico.")
    print("  -> scan_dbf_reporte_ultimo.html es el de referencia rapida.")
    print("  -> mapa_homologacion.csv es el insumo de 03T y 03N.")
    print("=" * 60)


if __name__ == "__main__":
    main()
