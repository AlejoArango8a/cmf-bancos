#!/usr/bin/env python3
"""
migrate_to_cockroachdb.py
Migra todos los datos de Supabase → CockroachDB.

Requiere en .env:
  SUPABASE_URL   = https://xxxx.supabase.co
  SUPABASE_KEY   = service_role key
  COCKROACH_URL  = postgresql://user:password@host:26257/defaultdb?sslmode=require
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from supabase import create_client

load_dotenv(Path(__file__).parent / ".env")

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
COCKROACH_URL = os.environ["COCKROACH_URL"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ============================================================
# SCHEMA — mismas tablas que Supabase
# ============================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS instituciones (
    codigo        INT  PRIMARY KEY,
    razon_social  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_cuentas (
    cuenta       TEXT PRIMARY KEY,
    descripcion  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS carga_log (
    periodo              TEXT PRIMARY KEY,
    archivos_procesados  INT  DEFAULT 0,
    estado               TEXT NOT NULL DEFAULT 'ok'
);

CREATE TABLE IF NOT EXISTS datos_financieros (
    periodo      TEXT   NOT NULL,
    tipo         TEXT   NOT NULL,
    ins_cod      INT    NOT NULL,
    cuenta       TEXT   NOT NULL,
    monto_clp    BIGINT DEFAULT 0,
    monto_uf     BIGINT DEFAULT 0,
    monto_tc     BIGINT DEFAULT 0,
    monto_ext    BIGINT DEFAULT 0,
    monto_total  BIGINT DEFAULT 0,
    PRIMARY KEY (periodo, tipo, ins_cod, cuenta)
);
"""

# ============================================================
# HELPERS
# ============================================================
PAGE_SIZE = 1000

def leer_supabase(sb, table, select="*"):
    """Lee todos los registros de una tabla Supabase con paginación."""
    all_rows = []
    offset = 0
    while True:
        resp = sb.table(table).select(select).range(offset, offset + PAGE_SIZE - 1).execute()
        rows = resp.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows

def insertar_batch(cur, sql, rows, batch_size=500):
    """Inserta filas en batches para no saturar la conexión."""
    for i in range(0, len(rows), batch_size):
        psycopg2.extras.execute_values(cur, sql, rows[i:i+batch_size])

# ============================================================
# MIGRACIÓN
# ============================================================
def migrate():
    log.info("=== Iniciando migración Supabase → CockroachDB ===")

    log.info("Conectando a Supabase...")
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    log.info("Conectando a CockroachDB...")
    conn = psycopg2.connect(COCKROACH_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Crear tablas
    log.info("Creando schema en CockroachDB...")
    cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("  Schema listo.")

    # ── instituciones ──────────────────────────────────────────
    log.info("Migrando instituciones...")
    rows = leer_supabase(sb, "instituciones", "codigo,razon_social")
    if rows:
        insertar_batch(cur,
            "INSERT INTO instituciones (codigo, razon_social) VALUES %s "
            "ON CONFLICT (codigo) DO UPDATE SET razon_social = EXCLUDED.razon_social",
            [(r["codigo"], r["razon_social"]) for r in rows]
        )
        conn.commit()
    log.info(f"  instituciones: {len(rows)} filas")

    # ── plan_cuentas ───────────────────────────────────────────
    log.info("Migrando plan_cuentas...")
    rows = leer_supabase(sb, "plan_cuentas", "cuenta,descripcion")
    if rows:
        insertar_batch(cur,
            "INSERT INTO plan_cuentas (cuenta, descripcion) VALUES %s "
            "ON CONFLICT (cuenta) DO UPDATE SET descripcion = EXCLUDED.descripcion",
            [(r["cuenta"], r["descripcion"]) for r in rows]
        )
        conn.commit()
    log.info(f"  plan_cuentas: {len(rows)} filas")

    # ── carga_log ──────────────────────────────────────────────
    log.info("Migrando carga_log...")
    rows = leer_supabase(sb, "carga_log", "periodo,archivos_procesados,estado")
    if rows:
        insertar_batch(cur,
            "INSERT INTO carga_log (periodo, archivos_procesados, estado) VALUES %s "
            "ON CONFLICT (periodo) DO UPDATE SET "
            "archivos_procesados = EXCLUDED.archivos_procesados, estado = EXCLUDED.estado",
            [(r["periodo"], r.get("archivos_procesados", 0), r["estado"]) for r in rows]
        )
        conn.commit()
    log.info(f"  carga_log: {len(rows)} filas")

    # ── datos_financieros — paginación por período ─────────────
    # Evita timeouts de Supabase con OFFSET alto: leemos de a un mes a la vez.
    log.info("Migrando datos_financieros (por período)...")

    INSERT_SQL = """INSERT INTO datos_financieros
               (periodo, tipo, ins_cod, cuenta,
                monto_clp, monto_uf, monto_tc, monto_ext, monto_total)
               VALUES %s
               ON CONFLICT (periodo, tipo, ins_cod, cuenta) DO UPDATE SET
                 monto_clp   = EXCLUDED.monto_clp,
                 monto_uf    = EXCLUDED.monto_uf,
                 monto_tc    = EXCLUDED.monto_tc,
                 monto_ext   = EXCLUDED.monto_ext,
                 monto_total = EXCLUDED.monto_total"""

    # Leer todos los períodos disponibles en Supabase
    periodos_resp = sb.table("carga_log").select("periodo").eq("estado", "ok").order("periodo").execute()
    periodos_supabase = [r["periodo"] for r in periodos_resp.data]
    log.info(f"  Períodos en Supabase: {periodos_supabase}")

    # Migrar todos los períodos sin excepción (ON CONFLICT DO UPDATE es seguro para duplicados)
    pendientes = periodos_supabase
    log.info(f"  Migrando todos los períodos: {pendientes}")

    total = 0
    for periodo in pendientes:
        log.info(f"  Migrando período {periodo}...")
        offset = 0
        periodo_total = 0

        while True:
            resp = sb.table("datos_financieros").select(
                "periodo,tipo,ins_cod,cuenta,monto_clp,monto_uf,monto_tc,monto_ext,monto_total"
            ).eq("periodo", periodo).range(offset, offset + PAGE_SIZE - 1).execute()

            rows = resp.data
            if not rows:
                break

            # Reconectar si la conexión se cayó
            try:
                cur.execute("SELECT 1")
            except Exception:
                log.warning("  Conexión perdida, reconectando...")
                conn = psycopg2.connect(COCKROACH_URL)
                conn.autocommit = False
                cur = conn.cursor()

            insertar_batch(cur, INSERT_SQL,
                [(r["periodo"], r["tipo"], r["ins_cod"], r["cuenta"],
                  r.get("monto_clp", 0), r.get("monto_uf", 0),
                  r.get("monto_tc", 0), r.get("monto_ext", 0),
                  r.get("monto_total", 0)) for r in rows]
            )
            conn.commit()
            periodo_total += len(rows)
            offset        += PAGE_SIZE

            if len(rows) < PAGE_SIZE:
                break

        total += periodo_total
        log.info(f"  {periodo}: {periodo_total:,} filas migradas (total acumulado: {total:,})")

    log.info(f"  datos_financieros: {total:,} filas nuevas migradas")

    cur.close()
    conn.close()
    log.info("=== Migración completada exitosamente ===")

if __name__ == "__main__":
    migrate()
