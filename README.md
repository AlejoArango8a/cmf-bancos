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

## Países cubiertos

| País | Estado |
|------|--------|
| Chile | Activo (CMF, desde 2022) |
| Colombia | En desarrollo |
| Perú | En desarrollo |
| Uruguay | En desarrollo |
