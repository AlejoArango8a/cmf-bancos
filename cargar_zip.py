#!/usr/bin/env python3
"""
cargar_zip.py
─────────────────────────────────────────────────────────────────────────────
Carga un ZIP de la CMF en Supabase.

USO
  python cargar_zip.py <ruta_al_zip>            # período detectado automáticamente
  python cargar_zip.py <ruta_al_zip> 202503     # período indicado explícitamente

VARIABLES DE ENTORNO REQUERIDAS
  SUPABASE_URL   → URL del proyecto Supabase
  SUPABASE_KEY   → service_role key (NO la anon)

PASOS PREVIOS
  1. Descarga el ZIP mensual desde la CMF manualmente:
       https://www.cmfchile.cl/portal/estadisticas/617/w3-propertyvalue-28917.html
  2. Guárdalo en cualquier carpeta de tu equipo.
  3. Ejecuta este script apuntando al archivo.
─────────────────────────────────────────────────────────────────────────────
"""

import sys
import io
import zipfile
import logging
from pathlib import Path
from cmf_loader import get_supabase, get_loaded_periods, process_zip, detect_periodo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def main():
    # ── Argumentos ──────────────────────────────────────────────────────────
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    zip_path = Path(sys.argv[1])
    periodo_override = sys.argv[2] if len(sys.argv) >= 3 else None

    if not zip_path.exists():
        log.error(f"Archivo no encontrado: {zip_path}")
        sys.exit(1)

    if not zip_path.suffix.lower() == '.zip':
        log.error(f"El archivo debe ser un .zip: {zip_path}")
        sys.exit(1)

    # ── Leer ZIP ────────────────────────────────────────────────────────────
    zip_bytes = zip_path.read_bytes()
    log.info(f"ZIP leído: {zip_path.name} ({len(zip_bytes) / 1024:.1f} KB)")

    # ── Detectar período ────────────────────────────────────────────────────
    if periodo_override:
        periodo = periodo_override
        log.info(f"Período indicado manualmente: {periodo}")
    else:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            periodo = detect_periodo(zf)

        if not periodo:
            log.error(
                "No se pudo detectar el período desde el ZIP.\n"
                "  Indica el período explícitamente: python cargar_zip.py archivo.zip 202503"
            )
            sys.exit(1)
        log.info(f"Período detectado automáticamente: {periodo}")

    # ── Validar formato del período ──────────────────────────────────────────
    if not (len(periodo) == 6 and periodo.isdigit()):
        log.error(f"Formato de período inválido: '{periodo}'. Debe ser YYYYMM (ej: 202503)")
        sys.exit(1)

    # ── Conectar a Supabase ──────────────────────────────────────────────────
    from cmf_loader import SUPABASE_URL, SUPABASE_KEY
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error(
            "Faltan credenciales de Supabase.\n"
            "  1. Copia el archivo .env.example → .env\n"
            "  2. Abre .env y pega tu SUPABASE_URL y SUPABASE_KEY\n"
            "  3. Vuelve a ejecutar el script"
        )
        sys.exit(1)
    supabase = get_supabase()

    # ── Verificar si ya fue cargado ──────────────────────────────────────────
    loaded = get_loaded_periods(supabase)
    if periodo in loaded:
        log.warning(f"El período {periodo} ya está cargado en Supabase.")
        answer = input("¿Deseas sobreescribir los datos? [s/N]: ").strip().lower()
        if answer != 's':
            log.info("Operación cancelada.")
            sys.exit(0)

    # ── Procesar y subir ─────────────────────────────────────────────────────
    archivos = process_zip(zip_bytes, periodo, supabase)
    log.info(f"✓ Listo — {archivos} archivos cargados para el período {periodo}.")


if __name__ == "__main__":
    main()
