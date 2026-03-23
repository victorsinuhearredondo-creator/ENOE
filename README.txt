Pipeline ENOE — Colibri v5
==========================

SECCIÓN I: Datos y validación  [COMPLETA]
------------------------------------------
01_descarga.py          Descarga todo en una barrida: DBFs micro + validadores
                        Skip logic: no re-descarga si ya existe
                        Crea: tradicional/, nueva/, validadores_2/

02_scan_dbf.py          Escanea DBFs y genera mapa_homologacion.csv
                        Identifica correcciones vs documentación oficial INEGI
                        Correcciones detectadas: ENT≠EST, NIV_INS, PAR_C, BUSCAR5C

03T_build_trad.py       Constructor ENOE tradicional 2005T1–2020T1
                        Output: derivados/enoe_trad_YYYY_TQ.parquet  (61 archivos)

03N_build_nueva.py      Constructor Nueva ENOE 2023T1–2025T4
                        Output: derivados_nueva/enoe_nueva_YYYY_TQ.parquet  (12 archivos)
                        ENOEN (2020T3–2022T4) excluida por quiebre metodológico COVID-19
                        FIX: ENT ← CVE_ENT antes de construir ID_HOGAR

04T_validacion_v6.py    Valida tradicional — micro vs tabulados INEGI
                        Resultado: 61/61 OK  |  max diff TPART 0.013 pp  |  TDESOC 0.000 pp
                        Requiere: xlrd  (conda install -c conda-forge xlrd)

04N_validacion_v3.py    Valida nueva ENOE — micro vs tabulados INEGI
                        Resultado: 12/12 OK  |  max diff TPART 0.009 pp  |  TDESOC 0.000 pp
                        Ponderador: FAC_TRI (factor trimestral)

Ajuste único necesario
-----------------------
En cada script, cambiar BASE_DIR a tu ruta local:
  BASE_DIR = Path(r"C:\TU\RUTA\ENOE")

Requisitos
----------
  pip install requests pandas pyarrow openpyxl
  conda install -c conda-forge xlrd           ← para .xls de validadores

IDs canónicos
-------------
ID_HOGAR   = CD_A | ENT | CON | UPM | V_SEL | N_HOG  (+N_PRO_VIV en nueva)
ID_ROSTER  = N_REN  (o alias disponible según vintage)
ID_PERSONA = ID_HOGAR | ID_ROSTER

Validación de resultados
------------------------
ENOE Tradicional  2005T1–2020T1:
  TPART  — 61/61 trimestres dentro de ±0.5 pp  |  media diff: -0.003 pp
  TDESOC — 61/61 trimestres dentro de ±0.5 pp  |  media diff: -0.000 pp

Nueva ENOE  2023T1–2025T4:
  TPART  — 12/12 trimestres dentro de ±0.5 pp  |  media diff: -0.005 pp
  TDESOC — 12/12 trimestres dentro de ±0.5 pp  |  media diff: -0.000 pp

Las diferencias residuales (~0.01 pp) son redondeo de los tabulados publicados,
no errores en los parquets.
