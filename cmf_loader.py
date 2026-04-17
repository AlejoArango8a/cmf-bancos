#!/usr/bin/env python3
"""
cmf_loader.py
Descarga ZIPs de la CMF, parsea los TXT y carga los datos en Supabase.
Puede correr en modo histórico (todos los meses) o incremental (último mes).
"""

import os
import re
import io
import zipfile
import requests
import logging
from datetime import datetime
from supabase import create_client, Client

# ============================================================
# CONFIGURACIÓN
# ============================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key (no la anon)

CMF_BASE = "https://www.cmfchile.cl/portal/estadisticas/617"
CMF_INDEX = f"{CMF_BASE}/w3-propertyvalue-28917.html"

# Todos los ZIPs disponibles en la CMF (extraídos de la página)
# Formato: (periodo_label, url_zip)
# Se genera dinámicamente leyendo la página de la CMF
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
# DESCUBRIR URLS DE LA CMF
# ============================================================
def get_cmf_urls() -> list[dict]:
    """Scrape la página CMF y devuelve lista de {periodo, url}"""
    log.info("Obteniendo lista de ZIPs desde CMF...")
    resp = requests.get(CMF_INDEX, timeout=30)
    resp.raise_for_status()

    # Extraer todos los links a ZIPs
    pattern = r'href="(/portal/estadisticas/617/articles-(\d+)_recurso_1\.zip)"'
    matches = re.findall(pattern, resp.text)

    # Extraer los períodos de los títulos adyacentes
    # El texto tiene pares: "Balance y Estado ... mes YYYY" seguido del link
    title_pattern = r'Balance y Estado de Situación Bancos (\w+ \d{4})'
    titles = re.findall(title_pattern, resp.text)

    MESES = {
        'enero':'01','febrero':'02','marzo':'03','abril':'04',
        'mayo':'05','junio':'06','julio':'07','agosto':'08',
        'septiembre':'09','octubre':'10','noviembre':'11','diciembre':'12'
    }

    results = []
    for i, (path, article_id) in enumerate(matches):
        url = f"https://www.cmfchile.cl{path}"
        # Inferir período del título correspondiente
        periodo = None
        if i < len(titles):
            parts = titles[i].lower().split()
            if len(parts) == 2:
                mes = MESES.get(parts[0])
                anio = parts[1]
                if mes and anio:
                    periodo = f"{anio}{mes}"
        results.append({"periodo": periodo, "url": url, "article_id": article_id})

    log.info(f"Encontrados {len(results)} ZIPs en CMF")
    return results

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

    # Primera línea: "001\tBANCO DE CHILE"
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
# PROCESAR UN ZIP
# ============================================================
def process_zip(zip_bytes: bytes, periodo: str, supabase: Client) -> int:
    """Procesa un ZIP y carga los datos en Supabase. Retorna número de archivos."""
    log.info(f"Procesando ZIP período {periodo}...")

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()

        # Leer metadata
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

        # Upsert instituciones
        if instituciones:
            rows = [{"codigo": k, "razon_social": v} for k, v in instituciones.items()]
            supabase.table("instituciones").upsert(rows).execute()
            log.info(f"  Instituciones: {len(rows)} registros")

        # Upsert plan de cuentas
        if plan_cuentas:
            rows = [{"cuenta": k, "descripcion": v} for k, v in plan_cuentas.items()]
            # Insert in batches
            for i in range(0, len(rows), BATCH_SIZE):
                supabase.table("plan_cuentas").upsert(rows[i:i+BATCH_SIZE]).execute()
            log.info(f"  Plan de cuentas: {len(rows)} registros")

        # Procesar archivos de datos
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
                continue  # Skip b2, c2 for now

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
                        "monto_clp": vals[0] if len(vals) > 0 else 0,
                        "monto_uf":  vals[1] if len(vals) > 1 else 0,
                        "monto_tc":  vals[2] if len(vals) > 2 else 0,
                        "monto_ext": vals[3] if len(vals) > 3 else 0,
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

        # Insert in batches
        log.info(f"  Insertando {len(all_rows)} filas ({file_count} archivos)...")
        for i in range(0, len(all_rows), BATCH_SIZE):
            supabase.table("datos_financieros").upsert(
                all_rows[i:i+BATCH_SIZE],
                on_conflict="periodo,tipo,ins_cod,cuenta"
            ).execute()

        # Log carga exitosa
        supabase.table("carga_log").upsert({
            "periodo": periodo,
            "archivos_procesados": file_count,
            "estado": "ok"
        }, on_conflict="periodo").execute()

        log.info(f"  ✓ Período {periodo} completado — {file_count} archivos, {len(all_rows)} filas")
        return file_count

# ============================================================
# MAIN
# ============================================================
def get_loaded_periods(supabase: Client) -> set:
    resp = supabase.table("carga_log").select("periodo").eq("estado", "ok").execute()
    return {r["periodo"] for r in resp.data}

def run(mode: str = "incremental"):
    """
    mode = "incremental": solo procesa períodos nuevos no cargados aún
    mode = "full": procesa todos los períodos desde 2023
    mode = "latest": solo el período más reciente
    """
    supabase = get_supabase()
    cmf_urls = get_cmf_urls()
    loaded = get_loaded_periods(supabase)

    log.info(f"Modo: {mode} | Períodos ya cargados: {len(loaded)}")

    # Filtrar según modo
    if mode == "full":
        # Desde enero 2023
        targets = [u for u in cmf_urls if u["periodo"] and u["periodo"] >= "202301"]
    elif mode == "latest":
        # Solo el más reciente
        valid = [u for u in cmf_urls if u["periodo"]]
        targets = [max(valid, key=lambda x: x["periodo"])] if valid else []
    else:  # incremental
        targets = [u for u in cmf_urls if u["periodo"] and u["periodo"] not in loaded and u["periodo"] >= "202301"]

    log.info(f"Períodos a procesar: {len(targets)}")

    for item in sorted(targets, key=lambda x: x["periodo"]):
        periodo = item["periodo"]
        url = item["url"]
        log.info(f"Descargando {periodo} desde {url}...")
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            process_zip(resp.content, periodo, supabase)
        except Exception as e:
            log.error(f"Error procesando {periodo}: {e}")
            supabase.table("carga_log").upsert({
                "periodo": periodo,
                "archivos_procesados": 0,
                "estado": f"error: {str(e)[:200]}"
            }, on_conflict="periodo").execute()

    log.info("✓ Proceso completado")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    run(mode)
