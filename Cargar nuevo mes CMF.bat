@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo.
echo  ╔══════════════════════════════════════════════╗
echo  ║      ALM BTG — Carga de datos CMF            ║
echo  ╚══════════════════════════════════════════════╝
echo.

:: ── Verificar que Python está instalado ─────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python no está instalado o no está en el PATH.
    echo  Descárgalo desde https://www.python.org
    echo.
    pause
    exit /b 1
)

:: ── Verificar que existe el archivo .env con credenciales ───────────────────
if not exist ".env" (
    echo  ERROR: No se encontró el archivo .env con las credenciales.
    echo.
    echo  Pasos para crearlo:
    echo    1. Busca el archivo ".env.example" en esta carpeta
    echo    2. Duplicalo y renombralo a ".env"
    echo    3. Abrelo con el Bloc de notas y pega tu SUPABASE_URL y SUPABASE_KEY
    echo.
    pause
    exit /b 1
)

:: ── Abrir selector de archivo ZIP ───────────────────────────────────────────
echo  Selecciona el archivo ZIP descargado desde la CMF...
echo.

for /f "usebackq delims=" %%F in (`powershell -noprofile -command ^
    "Add-Type -AssemblyName System.Windows.Forms; ^
     $d = New-Object System.Windows.Forms.OpenFileDialog; ^
     $d.Title = 'Selecciona el ZIP de la CMF'; ^
     $d.Filter = 'Archivos ZIP|*.zip'; ^
     $d.InitialDirectory = [Environment]::GetFolderPath('UserProfile') + '\Downloads'; ^
     if ($d.ShowDialog() -eq 'OK') { $d.FileName } else { 'CANCELADO' }"`) do set "ZIP_PATH=%%F"

if "%ZIP_PATH%"=="CANCELADO" (
    echo  Operación cancelada.
    echo.
    pause
    exit /b 0
)

echo  Archivo seleccionado: %ZIP_PATH%
echo.
echo  ─────────────────────────────────────────────────
echo  Iniciando carga en Supabase...
echo  ─────────────────────────────────────────────────
echo.

:: ── Ejecutar el script de carga ─────────────────────────────────────────────
python cargar_zip.py "%ZIP_PATH%"

echo.
echo  ─────────────────────────────────────────────────
if errorlevel 1 (
    echo  Hubo un error durante la carga. Revisa los mensajes arriba.
) else (
    echo  Carga completada exitosamente.
)
echo  ─────────────────────────────────────────────────
echo.
pause
