# ALM BTG — Banks Monitor

Dashboard de estados financieros de bancos chilenos. Fuente: CMF Chile · IFRS · Desde enero 2022.

---

## Estructura del proyecto

```
index.html                  Dashboard web
Cargar nuevo mes CMF.bat    ← Lo que usas cada mes para subir datos
cargar_zip.py               Script de carga (llamado por el .bat)
cmf_loader.py               Librería: parsers y carga a Supabase
.env                        Tus credenciales (no se sube a GitHub)
.env.example                Plantilla para crear el .env
requirements.txt            Dependencias Python
backend/                    API Express (Node.js) en Render
assets/                     Logos e imágenes del dashboard
```

---

## Setup inicial (solo la primera vez)

### 1. Instalar dependencias Python

Abre una terminal en la carpeta del proyecto y ejecuta:

```
pip install -r requirements.txt
```

### 2. Crear el archivo de credenciales

- Copia `.env.example` → `.env`
- Abre `.env` con el Bloc de notas
- Reemplaza los valores con tu `SUPABASE_URL` y `SUPABASE_KEY`  
  *(los encuentras en Supabase → Project Settings → API)*

---

## Cargar datos de un nuevo mes

1. Descarga el ZIP del mes desde la CMF:  
   https://www.cmfchile.cl/portal/estadisticas/617/w3-propertyvalue-28917.html

2. Haz **doble clic** en `Cargar nuevo mes CMF.bat`

3. Se abre el selector de archivos → elige el ZIP que descargaste

4. El script sube todo a Supabase automáticamente

---

## Seguridad — Supabase RLS

La `anon key` de Supabase está incluida en el bundle del navegador (es pública por diseño en proyectos estáticos). La única defensa real es que **Row Level Security esté bien configurado en Supabase**.

### Políticas RLS mínimas recomendadas

En Supabase → Table Editor → selecciona la tabla → Policies:

| Tabla | Operación permitida | Condición |
|---|---|---|
| `datos_financieros` | `SELECT` | `true` (lectura pública, sin escritura) |
| `instituciones` | `SELECT` | `true` |
| `plan_cuentas` | `SELECT` | `true` |
| `carga_log` | `SELECT` | `true` |
| `datos_financieros` | `INSERT / UPDATE / DELETE` | **Denegar** (ninguna policy = bloqueado) |
| Todas las tablas | `INSERT / UPDATE / DELETE` con anon | **Denegar** |

> **Importante:** habilita RLS en cada tabla (el toggle "Enable RLS" en Table Editor). Sin RLS activo, cualquier usuario con la anon key puede leer y escribir sin restricciones.

### Verificación rápida

```sql
-- Ejecuta en Supabase → SQL Editor para ver qué tablas tienen RLS activo
SELECT tablename, rowsecurity
FROM pg_tables
WHERE schemaname = 'public';
```

Todas las tablas de datos deben mostrar `rowsecurity = true`.

---

## CORS — Backend (Render)

El backend usa CORS **cerrado por defecto**. Solo acepta peticiones de los orígenes en `FRONTEND_URLS`.

En Render → Environment Variables, configura:
```
FRONTEND_URLS=https://alejoarango8a.github.io
```

Para añadir más orígenes (staging, local dev), sepáralos con coma:
```
FRONTEND_URLS=https://alejoarango8a.github.io,http://localhost:5500
```

---

## Países cubiertos

| País | Estado |
|------|--------|
| Chile | Activo (CMF, desde 2022) |
| Colombia | En desarrollo |
| Perú | En desarrollo |
| Uruguay | En desarrollo |
