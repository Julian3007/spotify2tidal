# Spotify2Tidal

Spotify2Tidal is a tool designed to help users seamlessly transfer their music data—including tracks, albums, artists, and playlists—from Spotify to TIDAL. It provides a simple command-line interface for exporting your Spotify data and importing it into your TIDAL account.

## Features
- Export tracks, albums, artists, and playlists from Spotify
- Import exported data into TIDAL
- Simple menu-driven interface
- Uses `.env` file for secure credential management

## Requirements
- Python 3.8 or higher
- Spotify Developer account (for API credentials)
- TIDAL account

## Setup
1. **Clone the repository**
2. **Install Python dependencies**
   - Run the batch script or manually install with:
     ```sh
     pip install -r requirements.txt
     ```
3. **Set up your credentials**
   - You will need your Spotify Client ID, Client Secret, and Redirect URI. Today (16.10.25) ```http://127.0.0.1:8080``` works. Get these from the [Spotify Developer Dashboard](https://developer.spotify.com/).
   - The batch script (`run_transfer.bat`) can help you set up your `.env` file interactively.


## How the Python Script Works

The main script, `spotify_tidal_transfer.py`, is an interactive command-line tool that guides you through the process of exporting your Spotify data and importing it into TIDAL. Here is an overview of its structure and features:

### Main Features
- **Interactive Menu:** Presents a menu to test connections, export data from Spotify, and import data into TIDAL.
- **Spotify Connection:** Uses your credentials from the `.env` file to authenticate with the Spotify API.
- **TIDAL Connection:** Guides you through TIDAL OAuth login in your browser.
- **Export Options:**
   - Export tracks, albums, artists, or playlists from your Spotify account to CSV files in the `exports` folder.
   - Export all data at once for convenience.
- **Import Options:**
   - Import tracks, playlists, albums, or artists from exported CSV files into your TIDAL account.
   - Automatically matches and adds tracks, albums, and artists as favorites or to playlists in TIDAL.
- **Logging:** All actions and errors are logged to `transfer.log` for troubleshooting.

### Script Structure
- The main logic is encapsulated in the `SpotifyTidalTransfer` class.
- The script loads environment variables from `.env` (using `python-dotenv`).
- The `run()` method presents the menu and handles user input.
- Each export/import function handles a specific data type (tracks, albums, artists, playlists).
- Data is exported as CSV files for easy review and backup.
- The script uses `spotipy` for Spotify API access, `tidalapi` for TIDAL, and `pandas` for data handling.

### Running the Script
You can run the main script directly:
```sh
python spotify_tidal_transfer.py
```
Or use the batch script for a guided setup and launch.

## .env File Example
```
SPOTIFY_CLIENT_ID=your_client_id_here
SPOTIFY_CLIENT_SECRET=your_client_secret_here
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback
```

## Logging
All actions and errors are logged to `transfer.log` for troubleshooting.

## License
This project is provided as-is for personal use.

## Disclaimer
This tool is not affiliated with Spotify or TIDAL. Use at your own risk. Always comply with the terms of service of both platforms.

