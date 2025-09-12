@echo off
setlocal enabledelayedexpansion
set ENV_FILE=.env

echo ========================================
echo Spotify2Tidal Setup Menu
echo ========================================
echo 1. Setup Spotify credentials
echo 2. Install Python requirements
echo 3. Run transfer script
echo 0. Exit
set /p MENU_CHOICE="Select an option: "

if "%MENU_CHOICE%"=="1" goto setup_creds
if "%MENU_CHOICE%"=="2" goto install_reqs
if "%MENU_CHOICE%"=="3" goto run_script
if "%MENU_CHOICE%"=="0" goto end
echo Invalid option.
goto end

:setup_creds
echo # Spotify API Credentials > %ENV_FILE%
set /p SPOTIFY_CLIENT_ID="Enter your Spotify Client ID: "
echo SPOTIFY_CLIENT_ID=!SPOTIFY_CLIENT_ID!>> %ENV_FILE%
set /p SPOTIFY_CLIENT_SECRET="Enter your Spotify Client Secret: "
echo SPOTIFY_CLIENT_SECRET=!SPOTIFY_CLIENT_SECRET!>> %ENV_FILE%
set /p SPOTIFY_REDIRECT_URI="Enter your Spotify Redirect URI (default: http://localhost:8888/callback): "
if "!SPOTIFY_REDIRECT_URI!"=="" set SPOTIFY_REDIRECT_URI=http://localhost:8888/callback
echo SPOTIFY_REDIRECT_URI=!SPOTIFY_REDIRECT_URI!>> %ENV_FILE%
echo Credentials saved to .env
goto end

:install_reqs
pip install -r requirements.txt
goto end

:run_script
python spotify_tidal_transfer.py
goto end

:end
endlocal
