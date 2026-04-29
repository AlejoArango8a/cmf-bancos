#!/usr/bin/env python3
"""
cmf_loader.py
Librería de procesamiento: parsea los TXT dentro de un ZIP de la CMF
y carga los datos en Supabase.

Uso directo: ver cargar_zip.py
"""

import os
import re
import io
import zipfile
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Carga automáticamente el archivo .env si existe en la raíz del proyecto
load_dotenv(Path(__file__).parent / ".env")

# ============================================================
# CONFIGURACIÓN
# ============================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")  # service_role key (no la anon)

BATCH_SIZE = 500  # filas por insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ============================================================
# SUPABASE CLIENT
# ============================================================
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# PARSERS
# ============================================================
def parse_plan_cuentas(text: str) -> dict:
    result = {}
    for line in text.splitlines():
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        cuenta = parts[0].strip()
        if re.match(r'^\d{9}$', cuenta):
            result[cuenta] = parts[1].strip()
    return result

def parse_instituciones(text: str) -> dict:
    result = {}
    for line in text.splitlines():
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        try:
            code = int(parts[0].strip())
            result[code] = parts[1].strip()
        except ValueError:
            continue
    return result

def parse_data_file(text: str, tipo: str) -> tuple[int, dict]:
    """
    Retorna (ins_code, {cuenta: valores})
    Para b1: valores = [clp, uf, tc, ext]
    Para r1/c1: valores = int
    """
    lines = text.splitlines()
    if not lines:
        return None, {}

    header = lines[0].split('\t')
    if len(header) < 2:
        return None, {}
    try:
        ins_code = int(header[0].strip())
    except ValueError:
        return None, {}

    is_multi = tipo in ('b1', 'b2')
    data = {}

    for line in lines[1:]:
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        cuenta = parts[0].strip()
        if not re.match(r'^\d{9}$', cuenta):
            continue

        if is_multi:
            vals = []
            for i in range(4):
                s = parts[i+1].strip() if i+1 < len(parts) else '0'
                try:
                    vals.append(int(s) if s else 0)
                except ValueError:
                    vals.append(0)
            data[cuenta] = vals
        else:
            s = parts[1].strip() if len(parts) > 1 else '0'
            try:
                data[cuenta] = int(s) if s else 0
            except ValueError:
                data[cuenta] = 0

    return ins_code, data

# ============================================================
# DETECTAR PERÍODO DESDE EL CONTENIDO DEL ZIP
# ============================================================
def detect_periodo(zf: zipfile.ZipFile) -> str | None:
    """
    Infiere el período (YYYYMM) desde los nombres de archivos de datos
    dentro del ZIP (ej: b1202503001.txt → '202503').
    """
    data_pattern = re.compile(r'^(b1|b2|r1|c1|c2)(\d{6})\d{3}\.txt$', re.IGNORECASE)
    for name in zf.namelist():
        fname = name.split('/')[-1]
        m = data_pattern.match(fname)
        if m:
            return m.group(2)
    return None

# ============================================================
# PROCESAR UN ZIP
# ============================================================
def process_zip(zip_bytes: bytes, periodo: str, supabase: Client) -> int:
    """Procesa un ZIP y carga los datos en Supabase. Retorna número de archivos."""
    log.info(f"Procesando ZIP período {periodo}...")

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()

        instituciones = {}
        plan_cuentas = {}

        for name in names:
            fname = name.split('/')[-1].lower()
            if fname == 'listado_instituciones.txt':
                text = zf.read(name).decode('utf-8', errors='replace')
                instituciones = parse_instituciones(text)
            elif fname == 'plan_de_cuentas.txt':
                text = zf.read(name).decode('utf-8', errors='replace')
                plan_cuentas = parse_plan_cuentas(text)

        if instituciones:
            rows = [{"codigo": k, "razon_social": v} for k, v in instituciones.items()]
            supabase.table("instituciones").upsert(rows).execute()
            log.info(f"  Instituciones: {len(rows)} registros")

        if plan_cuentas:
            rows = [{"cuenta": k, "descripcion": v} for k, v in plan_cuentas.items()]
            for i in range(0, len(rows), BATCH_SIZE):
                supabase.table("plan_cuentas").upsert(rows[i:i+BATCH_SIZE]).execute()
            log.info(f"  Plan de cuentas: {len(rows)} registros")

        data_pattern = re.compile(r'^(b1|b2|r1|c1|c2)(\d{6})(\d{3})\.txt$', re.IGNORECASE)
        file_count = 0
        all_rows = []

        for name in names:
            fname = name.split('/')[-1]
            m = data_pattern.match(fname)
            if not m:
                continue

            tipo = m.group(1).lower()
            if tipo not in ('b1', 'r1', 'c1'):
                continue  # Skip b2, c2

            text = zf.read(name).decode('utf-8', errors='replace')
            ins_code, data = parse_data_file(text, tipo)
            if ins_code is None:
                continue

            is_multi = tipo == 'b1'

            for cuenta, vals in data.items():
                if is_multi:
                    row = {
                        "periodo": periodo,
                        "tipo": tipo,
                        "ins_cod": ins_code,
                        "cuenta": cuenta,
                        "monto_clp":   vals[0] if len(vals) > 0 else 0,
                        "monto_uf":    vals[1] if len(vals) > 1 else 0,
                        "monto_tc":    vals[2] if len(vals) > 2 else 0,
                        "monto_ext":   vals[3] if len(vals) > 3 else 0,
                        "monto_total": sum(vals),
                    }
                else:
                    row = {
                        "periodo": periodo,
                        "tipo": tipo,
                        "ins_cod": ins_code,
                        "cuenta": cuenta,
                        "monto_clp": 0,
                        "monto_uf": 0,
                        "monto_tc": 0,
                        "monto_ext": 0,
                        "monto_total": vals,
                    }
                all_rows.append(row)

            file_count += 1

        log.info(f"  Insertando {len(all_rows)} filas ({file_count} archivos)...")
        for i in range(0, len(all_rows), BATCH_SIZE):
            supabase.table("datos_financieros").upsert(
                all_rows[i:i+BATCH_SIZE],
                on_conflict="periodo,tipo,ins_cod,cuenta"
            ).execute()

        supabase.table("carga_log").upsert({
            "periodo": periodo,
            "archivos_procesados": file_count,
            "estado": "ok"
        }, on_conflict="periodo").execute()

        log.info(f"  ✓ Período {periodo} completado — {file_count} archivos, {len(all_rows)} filas")
        return file_count

# ============================================================
# PERÍODOS YA CARGADOS
# ============================================================
def get_loaded_periods(supabase: Client) -> set:
    resp = supabase.table("carga_log").select("periodo").eq("estado", "ok").execute()
    return {r["periodo"] for r in resp.data}
