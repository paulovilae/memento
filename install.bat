@echo off
REM Memento Local Node Installer for Windows
REM Respects the ImagineOS PM2 mandate

echo 🧠 Building Memento Sovereign Memory Node...

REM 1. Build the Rust binary in release mode
cargo build --release
if %errorlevel% neq 0 (
    echo ❌ Cargo build failed. Ensure Rust is installed.
    exit /b %errorlevel%
)

REM 2. Setup local installation paths
set INSTALL_DIR=%USERPROFILE%\.local\bin
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"

echo 📦 Installing binary to %INSTALL_DIR%\memento.exe
copy target\release\memento.exe "%INSTALL_DIR%\memento.exe" /Y

REM 3. Setup PM2 (ImagineOS Standard)
echo 🚀 Configuring PM2 Process Manager...
pm2 -v >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ PM2 could not be found. Please install it with 'npm install -g pm2'
    exit /b 1
)

REM Stop it if it's already running
pm2 stop memento-node >nul 2>&1

REM Start or Restart the Memento Node
pm2 start ecosystem.config.cjs
pm2 save

echo ✅ Memento Local Node installation complete!
echo 🌐 View your Local Dashboard at: http://localhost:3306
echo 🗄️ Checking Memento Status:
pm2 status memento-node
pause
