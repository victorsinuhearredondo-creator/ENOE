Pipeline ENOE — Colibri v4
==========================

SECCIÓN I: Datos y validación
------------------------------
01_descarga.py      Descarga todo en una barrida: DBFs micro + validadores
                    Skip logic: no re-descarga si ya existe
                    Crea: tradicional/, nueva/, validadores_2/

02_scan_dbf.py      Escanea DBFs y genera mapa_homologacion.csv
                    Identifica correcciones vs documentación oficial INEGI

03T_build_trad.py   Constructor ENOE tradicional 2005T1–2020T1
                    Output: derivados/enoe_trad_YYYY_TQ.parquet (61 archivos)

03N_build_nueva.py  Constructor Nueva ENOE 2023T1–2025T4
                    Output: derivados_nueva/enoe_nueva_YYYY_TQ.parquet (12 archivos)
                    ENOEN (2020T3–2022T4) excluida por quiebre metodológico COVID-19

SECCIÓN II: Análisis y modelos (en desarrollo)
-----------------------------------------------
05_descriptivos.py       Perfil ENOE vs Censo 2020
06_boom_demografico.py   Presión demográfica por cohorte quinquenal (C1–C5)
07_transiciones.py       A1/A2/B/C — rotación y movilidad
08_attricion.py          Attrición diferencial
09_migracion.py          Migración inter-estatal como margen de ajuste
10_genero.py             Participación femenina y brecha salarial
11_jovenes_steam.py      Jóvenes STEAM
12_heckman.py            Selección por observabilidad (2 etapas)
13_deaton.py             Pseudo-panel Deaton por cohorte

Ajuste único necesario
-----------------------
En cada script, cambiar BASE_DIR a tu ruta local:
  BASE_DIR = Path(r"C:\TU\RUTA\ENOE")

IDs canónicos
-------------
ID_HOGAR   = CD_A | ENT | CON | UPM | V_SEL | N_HOG  (+N_PRO_VIV en nueva)
ID_ROSTER  = N_REN
ID_PERSONA = ID_HOGAR | ID_ROSTER
