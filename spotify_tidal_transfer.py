#!/usr/bin/env python3
"""
Spotify to TIDAL Transfer Tool
A clean, unified solution for transferring music data between Spotify and TIDAL

Features:
- Export from Spotify (tracks, artists, albums, playlists)
- Import to TIDAL (with playlist structure preservation)
- Interactive menu interface
- Connection testing for both services

Author: Generated for Julian Meyer
Date: September 12, 2025
"""

import os
import sys
import json
import csv
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Third-party imports
try:
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    import tidalapi
    from tqdm import tqdm
except ImportError as e:
    print(f"❌ Missing required package: {e}")
    print("📦 Please install with: pip install spotipy tidalapi tqdm pandas")
    sys.exit(1)


class SpotifyTidalTransfer:

    def connect_spotify(self):
        """Connect to Spotify and set self.spotify. Returns True if successful."""
        try:
            client_id = os.getenv("SPOTIFY_CLIENT_ID")
            client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
            redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8888/callback")
            # Small, safe debug output to help troubleshoot missing/incorrect env vars or redirect URI mismatches.
            def _mask(val: Optional[str]) -> str:
                if not val:
                    return "(missing)"
                v = str(val)
                if len(v) <= 4:
                    return "****"
                return "*" * (len(v) - 4) + v[-4:]

            if not client_id or not client_secret:
                print("❌ Spotify credentials not set. Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file.")
                print(f"🔎 SPOTIFY_CLIENT_ID present: {bool(client_id)} (masked: {_mask(client_id)})")
                print(f"🔎 SPOTIFY_REDIRECT_URI: {redirect_uri!r}")
                return False
            # Show which client_id (masked) and redirect uri are being used so user can confirm dashboard settings.
            print(f"🔎 SPOTIFY_CLIENT_ID present: {bool(client_id)} (masked: {_mask(client_id)})")
            print(f"🔎 SPOTIFY_REDIRECT_URI: {redirect_uri!r}")
            scope = "user-library-read playlist-read-private user-follow-read"
            auth_manager = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope)
            # Try normal auth manager flow first
            try:
                self.spotify = spotipy.Spotify(auth_manager=auth_manager)
                user = self.spotify.current_user()
                print(f"✅ Connected to Spotify as {user.get('display_name', user['id'])}")
                return True
            except Exception:
                # Fall back to manual auth flow when redirect handling (e.g. https localhost) fails
                try:
                    import webbrowser
                    print("🔁 Falling back to manual OAuth flow. A browser window will open; after you authorize, paste the full redirect URL here.")
                    auth_url = auth_manager.get_authorize_url()
                    print(f"🔗 Authorization URL: {auth_url}")
                    try:
                        webbrowser.open(auth_url)
                    except Exception:
                        pass

                    # Prompt the user and validate that they pasted the redirect URL containing the authorization code.
                    code = None
                    for attempt in range(3):
                        redirect_response = input("Paste the full redirect URL after authorizing (the URL in your browser address bar): ").strip()
                        # Try spotipy helper first
                        try:
                            code = auth_manager.parse_response_code(redirect_response)
                        except Exception:
                            code = None
                        # Fallback: parse query string for ?code= if parse_response_code didn't work
                        if not code:
                            try:
                                from urllib.parse import urlparse, parse_qs
                                parsed = urlparse(redirect_response)
                                qs = parse_qs(parsed.query)
                                if 'code' in qs:
                                    code = qs['code'][0]
                            except Exception:
                                code = None

                        if not code:
                            print("⚠️ It looks like the pasted URL does not contain an authorization code. Make sure you paste the final redirect URL (it should contain 'code=').")
                            if attempt < 2:
                                print("Please try again.")
                                continue
                            else:
                                print("Aborting manual OAuth after 3 failed attempts.")
                                raise Exception("No authorization code provided by user")

                    token_info = auth_manager.get_access_token(code)
                    # token_info may be a dict or an access token string depending on spotipy version
                    access_token = token_info['access_token'] if isinstance(token_info, dict) and 'access_token' in token_info else token_info
                    if not access_token:
                        print("❌ Could not obtain access token from the provided response.")
                        return False
                    self.spotify = spotipy.Spotify(auth=access_token)
                    user = self.spotify.current_user()
                    print(f"✅ Connected to Spotify as {user.get('display_name', user['id'])}")
                    return True
                except Exception as exc:
                    print(f"❌ Manual OAuth flow failed: {exc}")
                    self.spotify = None
                    return False
            return True
        except Exception as e:
            # Provide extra hint for invalid_client errors without exposing secrets
            msg = str(e)
            print(f"❌ Failed to connect to Spotify: {msg}")
            if 'invalid_client' in msg.lower() or 'invalid client' in msg.lower():
                print("   ℹ️ Common causes: redirect URI mismatch in Spotify Dashboard, incorrect client id/secret, or using the wrong app.")
            self.spotify = None
            return False

    def connect_tidal(self):
        """Connect to TIDAL and set self.tidal. Returns True if successful."""
        try:
            import tidalapi as _tidal
            version = getattr(_tidal, '__version__', 'unknown')
            print(f"� tidalapi version: {version}")
            session = _tidal.Session()
            print("🔑 Initiating TIDAL login (device OAuth)...")
            try:
                session.login_oauth_simple()
            except Exception as inner_e:
                msg = str(inner_e)
                if 'invalid_client' in msg.lower():
                    print("❌ TIDAL authentication reported invalid_client.")
                    print("   Possible causes:")
                    print("   1. Outdated tidalapi library (try: pip install --upgrade tidalapi)")
                    print("   2. TIDAL changed public client credentials used by tidalapi (update library)")
                    print("   3. Regional restrictions or account type not supported")
                    print("   4. Temporary TIDAL auth service issue")
                    print("   Next steps:")
                    print("     a) Upgrade: pip install --upgrade tidalapi")
                    print("     b) If still failing, file an issue at https://github.com/tidalapi/tidalapi with the error.")
                raise
            self.tidal = session
            username = getattr(getattr(self.tidal, 'user', None), 'username', None)
            print(f"✅ Connected to TIDAL as {username or 'Unknown'}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to TIDAL: {e}")
            self.tidal = None
            return False
    def __init__(self):
        self.spotify = None
        self.tidal = None
        self.output_dir = Path("exports")
        self.output_dir.mkdir(exist_ok=True)
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("transfer.log"),
                logging.StreamHandler()
            ]
        )

    def show_main_menu(self):
        """Display the main menu and handle user input"""
        print("\n🎵 MAIN MENU")
        print("=" * 40)
        print("1. Test Connections")
        print("   🔧 1a) Test Spotify connection")
        print("   🔧 1b) Test TIDAL connection")
        print("   🔧 1c) Test both connections")
        print("")
        print("2. Export from Spotify")
        print("   📥 2a) Export tracks")
        print("   📥 2b) Export albums")
        print("   📥 2c) Export artists")
        print("   📥 2d) Export playlists")
        print("   📥 2e) Export all (tracks, albums, artists, playlists)")
        print("")
        print("3. Import to TIDAL")
        print("   📤 3) List available CSV files and import from selection")
        print("")
        print("0. Exit")
        print("=" * 40)

    def run(self):
        """Main application loop"""
        while True:
            self.show_main_menu()
            choice = input("\nSelect option: ").strip().lower()

            if choice == "1a":
                self.connect_spotify()
            elif choice == "1b":
                self.connect_tidal()
            elif choice == "1c" or choice == "1":
                self.test_connections()
            elif choice == "2a":
                if not hasattr(self, 'spotify') or self.spotify is None:
                    if not self.connect_spotify():
                        continue
                self.export_spotify_tracks()
            elif choice == "2b":
                if not self.spotify:
                    if not self.connect_spotify():
                        continue
                # Export saved albums only
                print("\nExporting saved albums...")
                all_albums = []
                try:
                    offset = 0
                    limit = 50
                    while True:
                        results = self.spotify.current_user_saved_albums(limit=limit, offset=offset)
                        items = results['items']
                        if not items:
                            break
                        for item in items:
                            album = item['album']
                            album_data = {
                                'album_id': album['id'],
                                'album_name': album['name'],
                                'album_url': album['external_urls']['spotify'],
                                'artist_name': ', '.join([artist['name'] for artist in album['artists']])
                            }
                            all_albums.append(album_data)
                        offset += limit
                        if len(items) < limit:
                            break
                    if all_albums:
                        import pandas as pd
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = self.output_dir / f"spotify_albums_{timestamp}.csv"
                        df = pd.DataFrame(all_albums)
                        df.to_csv(filename, index=False, encoding='utf-8')
                        print(f"✅ Albums export saved to: {filename}")
                    else:
                        print("❌ No saved albums found.")
                except Exception as e:
                    print(f"❌ Error exporting saved albums: {e}")
            elif choice == "2c":
                if not self.spotify:
                    if not self.connect_spotify():
                        continue
                # Export followed artists only
                print("\nExporting followed artists...")
                all_artists = []
                try:
                    after = None
                    while True:
                        results = self.spotify.current_user_followed_artists(limit=50, after=after)
                        artists = results['artists']['items']
                        if not artists:
                            break
                        for artist in artists:
                            artist_data = {
                                'artist_id': artist['id'],
                                'artist_name': artist['name'],
                                'artist_url': artist['external_urls']['spotify']
                            }
                            all_artists.append(artist_data)
                        if len(artists) < 50:
                            break
                        after = artists[-1]['id']
                    if all_artists:
                        import pandas as pd
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = self.output_dir / f"spotify_artists_{timestamp}.csv"
                        df = pd.DataFrame(all_artists)
                        df.to_csv(filename, index=False, encoding='utf-8')
                        print(f"✅ Artists export saved to: {filename}")
                    else:
                        print("❌ No followed artists found.")
                except Exception as e:
                    print(f"❌ Error exporting followed artists: {e}")
            elif choice == "2d":
                if not self.spotify:
                    if not self.connect_spotify():
                        continue
                self.export_spotify_playlists()
            elif choice == "2e":
                if not self.spotify:
                    if not self.connect_spotify():
                        continue
                self.export_spotify_all()
            elif choice == "3":
                # Always list CSV files from exports and allow user to select one for import
                csv_files = self.list_csv_files()
                if not csv_files:
                    print("❌ No CSV files found in 'exports' directory.")
                else:
                    print("\nAvailable CSV files:")
                    for i, f in enumerate(csv_files):
                        print(f"{i+1}. {f}")
                    file_choice = input("Select file number to import: ").strip()
                    try:
                        file_choice = int(file_choice) - 1
                        if 0 <= file_choice < len(csv_files):
                            selected_file = csv_files[file_choice]
                            import pandas as pd
                            try:
                                df = pd.read_csv(selected_file, nrows=1)
                                cols = set(df.columns.str.lower())
                                # Determine file type by columns
                                if {'name', 'artist'}.issubset(cols):
                                    # Tracks/Playlists file
                                    self.import_to_tidal(selected_file, None, None)
                                elif {'artist_name', 'artist_id'}.issubset(cols):
                                    # Artists file
                                    self.import_to_tidal(None, selected_file, None)
                                elif {'album_name', 'album_id'}.issubset(cols):
                                    # Albums file
                                    self.import_to_tidal(None, None, selected_file)
                                else:
                                    print("❌ Could not determine file type. Please select a valid export CSV.")
                            except Exception as e:
                                print(f"❌ Error reading CSV: {e}")
                        else:
                            print("❌ Invalid selection.")
                    except ValueError:
                        print("❌ Invalid input.")
            elif choice == "0":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid option. Please try again.")
            input("\nPress Enter to continue...")

    
    def test_connections(self):
        """Test connections to both services"""
        print("\n🧪 Testing Connections")
        print("-" * 30)
        
        # Test Spotify
        if self.spotify is None:
            spotify_ok = self.connect_spotify()
        else:
            try:
                user = self.spotify.current_user()
                print(f"✅ Spotify: Connected as {user.get('display_name', user['id'])}")
                spotify_ok = True
            except:
                print("❌ Spotify: Connection lost")
                spotify_ok = False
        
        # Test TIDAL
        if self.tidal is None:
            tidal_ok = self.connect_tidal()
        else:
            try:
                username = getattr(self.tidal.user, 'username', 'Unknown')
                print(f"✅ TIDAL: Connected as {username}")
                tidal_ok = True
            except:
                print("❌ TIDAL: Connection lost")
                tidal_ok = False
        
        print(f"\n📊 Status: Spotify {'✅' if spotify_ok else '❌'} | TIDAL {'✅' if tidal_ok else '❌'}")
        return spotify_ok, tidal_ok
    
    def export_spotify_tracks(self) -> str:
        """Export user's saved tracks from Spotify"""
        if not self.spotify:
            print("❌ Spotify not connected")
            return ""
        
        print("\n📥 Exporting Spotify saved tracks...")
        
        tracks_data = []
        offset = 0
        limit = 50
        
        with tqdm(desc="Loading tracks") as pbar:
            while True:
                try:
                    results = self.spotify.current_user_saved_tracks(limit=limit, offset=offset)
                    items = results['items']
                    
                    if not items:
                        break
                    
                    for item in items:
                        track = item['track']
                        if track and track['type'] == 'track':
                            track_data = {
                                'name': track['name'],
                                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                                'album': track['album']['name'],
                                'playlist': 'Liked Songs',
                                'spotify_id': track['id'],
                                'spotify_url': track['external_urls']['spotify'],
                                'duration_ms': track['duration_ms'],
                                'popularity': track['popularity'],
                                'added_at': item['added_at']
                            }
                            tracks_data.append(track_data)
                    
                    pbar.update(len(items))
                    offset += limit
                    
                    if len(items) < limit:
                        break
                        
                except Exception as e:
                    print(f"❌ Error fetching tracks: {e}")
                    break
        
        if tracks_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = self.output_dir / f"spotify_tracks_{timestamp}.csv"
            
            df = pd.DataFrame(tracks_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            
            print(f"✅ Exported {len(tracks_data)} tracks to: {filename}")
            return str(filename)
        else:
            print("❌ No tracks found")
            return ""
    
    def export_spotify_playlists(self) -> str:
        """Export user's playlists from Spotify"""
        if not self.spotify:
            print("❌ Spotify not connected")
            return ""
        
        print("\n📥 Exporting Spotify playlists...")
        
        all_tracks = []
        playlists_info = []
        
        # Get user's playlists
        playlists = self.spotify.current_user_playlists(limit=50)
        
        with tqdm(desc="Loading playlists") as pbar:
            for playlist in playlists['items']:
                if playlist is None:
                    continue
                
                playlist_name = playlist['name']
                playlist_id = playlist['id']
                
                playlists_info.append({
                    'name': playlist_name,
                    'id': playlist_id,
                    'owner': playlist['owner']['display_name'],
                    'tracks_total': playlist['tracks']['total'],
                    'public': playlist['public'],
                    'spotify_url': playlist['external_urls']['spotify']
                })
                
                # Get tracks from this playlist
                try:
                    offset = 0
                    while True:
                        tracks = self.spotify.playlist_tracks(
                            playlist_id, 
                            limit=100, 
                            offset=offset
                        )
                        
                        for item in tracks['items']:
                            track = item.get('track')
                            if track and track.get('type') == 'track':
                                track_data = {
                                    'name': track['name'],
                                    'artist': ', '.join([artist['name'] for artist in track['artists']]),
                                    'album': track['album']['name'],
                                    'playlist': playlist_name,
                                    'spotify_id': track['id'],
                                    'spotify_url': track['external_urls']['spotify'],
                                    'duration_ms': track['duration_ms'],
                                    'popularity': track['popularity'],
                                    'playlist_id': playlist_id
                                }
                                all_tracks.append(track_data)
                        
                        if not tracks['next']:
                            break
                        
                        offset += 100
                        
                except Exception as e:
                    print(f"⚠️ Error loading playlist '{playlist_name}': {e}")
                
                pbar.update(1)
        
        if all_tracks:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save tracks
            tracks_filename = self.output_dir / f"spotify_playlists_tracks_{timestamp}.csv"
            df_tracks = pd.DataFrame(all_tracks)
            df_tracks.to_csv(tracks_filename, index=False, encoding='utf-8')
            
            # Save playlist info
            playlists_filename = self.output_dir / f"spotify_playlists_info_{timestamp}.csv"
            df_playlists = pd.DataFrame(playlists_info)
            df_playlists.to_csv(playlists_filename, index=False, encoding='utf-8')
            
            print(f"✅ Exported {len(all_tracks)} tracks from {len(playlists_info)} playlists")
            print(f"📄 Tracks: {tracks_filename}")
            print(f"📄 Playlist info: {playlists_filename}")
            
            return str(tracks_filename)
        else:
            print("❌ No playlist tracks found")
            return ""
    
    def export_spotify_all(self) -> str:
        """Export all Spotify data (tracks + playlists)"""
        print("\n📥 Exporting ALL Spotify data...")
        all_tracks = []
        all_artists = []
        all_albums = []
        
        # Export saved tracks
        print("1️⃣ Exporting saved tracks...")
        if self.spotify:
            try:
                offset = 0
                limit = 50
                while True:
                    results = self.spotify.current_user_saved_tracks(limit=limit, offset=offset)
                    items = results['items']
                    if not items:
                        break
                    for item in items:
                        track = item['track']
                        if track and track['type'] == 'track':
                            track_data = {
                                'name': track['name'],
                                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                                'album': track['album']['name'],
                                'playlist': 'Liked Songs',
                                'spotify_id': track['id'],
                                'spotify_url': track['external_urls']['spotify'],
                                'duration_ms': track['duration_ms'],
                                'popularity': track['popularity'],
                                'added_at': item['added_at']
                            }
                            all_tracks.append(track_data)
                    offset += limit
                    if len(items) < limit:
                        break
                print(f"✅ Found {len(all_tracks)} saved tracks")
            except Exception as e:
                print(f"❌ Error fetching saved tracks: {e}")

        # Export followed artists
        print("3️⃣ Exporting followed artists...")
        try:
            after = None
            while True:
                results = self.spotify.current_user_followed_artists(limit=50, after=after)
                artists = results['artists']['items']
                if not artists:
                    break
                for artist in artists:
                    artist_data = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'artist_url': artist['external_urls']['spotify']
                    }
                    all_artists.append(artist_data)
                # Spotify API: get next page using last artist's ID
                if len(artists) < 50:
                    break
                after = artists[-1]['id']
            print(f"✅ Found {len(all_artists)} followed artists")
        except Exception as e:
            print(f"❌ Error fetching followed artists: {e}")

        # Export saved albums
        print("4️⃣ Exporting saved albums...")
        try:
            offset = 0
            limit = 50
            while True:
                results = self.spotify.current_user_saved_albums(limit=limit, offset=offset)
                items = results['items']
                if not items:
                    break
                for item in items:
                    album = item['album']
                    album_data = {
                        'album_id': album['id'],
                        'album_name': album['name'],
                        'album_url': album['external_urls']['spotify'],
                        'artist_name': ', '.join([artist['name'] for artist in album['artists']])
                    }
                    all_albums.append(album_data)
                offset += limit
                if len(items) < limit:
                    break
            print(f"✅ Found {len(all_albums)} saved albums")
        except Exception as e:
            print(f"❌ Error fetching saved albums: {e}")
        
        # Export playlists
        print("2️⃣ Exporting playlists...")
        try:
            playlists = self.spotify.current_user_playlists(limit=50)
            
            for playlist in playlists['items']:
                if playlist is None:
                    continue
                
                playlist_name = playlist['name']
                playlist_id = playlist['id']
                
                print(f"   📂 Loading: {playlist_name}")
                
                # Get tracks from this playlist
                offset = 0
                while True:
                    tracks = self.spotify.playlist_tracks(
                        playlist_id, 
                        limit=100, 
                        offset=offset
                    )
                    
                    for item in tracks['items']:
                        track = item.get('track')
                        if track and track.get('type') == 'track':
                            track_data = {
                                'name': track['name'],
                                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                                'album': track['album']['name'],
                                'playlist': playlist_name,
                                'spotify_id': track['id'],
                                'spotify_url': track['external_urls']['spotify'],
                                'duration_ms': track['duration_ms'],
                                'popularity': track['popularity'],
                                'playlist_id': playlist_id
                            }
                            all_tracks.append(track_data)
                    
                    if not tracks['next']:
                        break
                    
                    offset += 100
            
            print(f"✅ Total tracks found: {len(all_tracks)}")
            
        except Exception as e:
            print(f"❌ Error fetching playlists: {e}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_files = {}
        if all_tracks:
            filename = self.output_dir / f"spotify_export_complete_{timestamp}.csv"
            df = pd.DataFrame(all_tracks)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Complete export saved to: {filename}")
            output_files['tracks'] = str(filename)
        if all_artists:
            filename = self.output_dir / f"spotify_artists_{timestamp}.csv"
            df = pd.DataFrame(all_artists)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Artists export saved to: {filename}")
            output_files['artists'] = str(filename)
        if all_albums:
            filename = self.output_dir / f"spotify_albums_{timestamp}.csv"
            df = pd.DataFrame(all_albums)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Albums export saved to: {filename}")
            output_files['albums'] = str(filename)
        if output_files:
            return json.dumps(output_files)
        else:
            print("❌ No data found to export")
            return ""
    
    def _normalize_search_text(self, text: str) -> str:
        """Normalize text for better search matching."""
        import re
        if not text:
            return ""
        
        # Remove content in parentheses/brackets (feat., live, remaster, etc.)
        text = re.sub(r'\([^)]*\)', '', text)
        text = re.sub(r'\[[^\]]*\]', '', text)
        
        # Remove common noise words
        noise_words = ['feat.', 'ft.', 'featuring', 'with', 'vs.', 'vs', '&']
        for word in noise_words:
            text = re.sub(r'\b' + re.escape(word) + r'\b', '', text, flags=re.IGNORECASE)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        return text.strip()
    
    def _extract_primary_artist(self, artist_str: str) -> str:
        """Extract the primary (first) artist from a string that may contain multiple artists."""
        if not artist_str:
            return ""
        
        # Split on common separators
        for sep in [',', ';', '&', ' and ', ' x ', ' X ']:
            if sep in artist_str:
                return artist_str.split(sep)[0].strip()
        
        return artist_str.strip()
    
    def _calculate_artist_similarity(self, artist1: str, artist2: str) -> float:
        """Calculate simple similarity score between two artist names (0.0 to 1.0)."""
        if not artist1 or not artist2:
            return 0.0
        
        a1 = artist1.lower().strip()
        a2 = artist2.lower().strip()
        
        # Exact match
        if a1 == a2:
            return 1.0
        
        # One contains the other
        if a1 in a2 or a2 in a1:
            return 0.9
        
        # Check if primary artist matches (useful for "Artist feat. Other")
        primary1 = self._extract_primary_artist(a1)
        primary2 = self._extract_primary_artist(a2)
        if primary1 == primary2:
            return 0.85
        
        # Simple word overlap
        words1 = set(a1.split())
        words2 = set(a2.split())
        if words1 and words2:
            overlap = len(words1 & words2) / max(len(words1), len(words2))
            return overlap * 0.7
        
        return 0.0
    
    def search_tidal_track(self, track_name: str, artist_name: str, album_name: str = None, duration_ms: int = None) -> Optional[Dict]:
        """
        Search for a track on TIDAL using progressive fallback queries and smart result filtering.
        Returns the best match as a dict with 'track', 'confidence', and 'query_used' keys, or None if not found.
        """
        import time
        
        # Normalize inputs
        track_clean = self._normalize_search_text(track_name)
        artist_clean = self._normalize_search_text(artist_name)
        primary_artist = self._extract_primary_artist(artist_clean)
        
        if not track_clean:
            return None
        
        # Progressive fallback queries (from most specific to least specific)
        queries = [
            f'"{track_clean}" "{primary_artist}"' if primary_artist else None,
            f'{track_clean} {primary_artist}' if primary_artist else None,
            f'{track_clean} {artist_clean}' if artist_clean and artist_clean != primary_artist else None,
            track_clean
        ]
        queries = [q for q in queries if q]  # Remove None entries
        
        best_match = None
        best_score = 0.0
        query_used = None
        
        for query in queries:
            try:
                search_result = self.tidal.search(query, limit=10)
                tracks = search_result.get('tracks', [])
                
                if not tracks:
                    continue
                
                # Score each result
                for track in tracks:
                    score = 0.0
                    
                    # Artist similarity (most important)
                    track_artist = track.artist.name if hasattr(track, 'artist') and track.artist else ""
                    artist_sim = self._calculate_artist_similarity(artist_name, track_artist)
                    score += artist_sim * 0.6
                    
                    # Album match (if available)
                    if album_name and hasattr(track, 'album') and track.album:
                        album_sim = self._calculate_artist_similarity(album_name, track.album.name)
                        score += album_sim * 0.2
                    
                    # Duration match (if available)
                    if duration_ms and hasattr(track, 'duration') and track.duration:
                        duration_diff = abs(duration_ms - track.duration * 1000)
                        if duration_diff < 5000:  # Within 5 seconds
                            score += 0.2
                        elif duration_diff < 15000:  # Within 15 seconds
                            score += 0.1
                    else:
                        score += 0.1  # Small bonus if no duration to compare
                    
                    # Track name similarity (basic check)
                    track_name_clean = self._normalize_search_text(track.name if hasattr(track, 'name') else "")
                    if track_clean.lower() in track_name_clean.lower() or track_name_clean.lower() in track_clean.lower():
                        score += 0.1
                    
                    # Update best match
                    if score > best_score:
                        best_score = score
                        best_match = track
                        query_used = query
                
                # If we found a high-confidence match, stop searching
                if best_score >= 0.8:
                    break
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"    ⚠️ Search error for query '{query}': {e}")
                continue
        
        # Only return matches with reasonable confidence
        if best_match and best_score >= 0.5:
            return {
                'track': best_match,
                'confidence': best_score,
                'query_used': query_used
            }
        
        return None

    def import_to_tidal(self, tracks_csv: str = None, artists_csv: str = None, albums_csv: str = None, mode: str = "all"):
        """Import tracks, followed artists, and albums to TIDAL from CSV files. Handles single-type files gracefully."""
        import pandas as pd
        import time
        imported = 0
        failed = 0
        failed_tracks = []  # Track failed imports for later review
        # Import tracks and playlists
        if tracks_csv:
            try:
                df = pd.read_csv(tracks_csv, encoding='utf-8')
                playlist_col = 'playlist' if 'playlist' in df.columns else None
                processed_idx = set()
                if playlist_col:
                    for playlist_name, group in df.groupby(playlist_col):
                        if not playlist_name or playlist_name.lower() == 'liked songs':
                            continue
                        print(f"\n▶️ Importing playlist: {playlist_name} ({len(group)} tracks)")
                        try:
                            user_playlists = self.tidal.user.playlists()
                            tidal_playlist = next((pl for pl in user_playlists if pl.name == playlist_name), None)
                            if not tidal_playlist:
                                tidal_playlist = self.tidal.user.create_playlist(playlist_name, description="Imported from Spotify")
                                print(f"  ➕ Created TIDAL playlist: {playlist_name}")
                            else:
                                print(f"  ℹ️ Using existing TIDAL playlist: {playlist_name}")
                        except Exception as e:
                            print(f"❌ Error creating/finding playlist '{playlist_name}': {e}")
                            continue
                        batch_ids = []
                        for idx, row in group.iterrows():
                            processed_idx.add(idx)
                            try:
                                # Use enhanced search with validation
                                match_result = self.search_tidal_track(
                                    track_name=row['name'],
                                    artist_name=row['artist'],
                                    album_name=row.get('album'),
                                    duration_ms=row.get('duration_ms')
                                )
                                
                                if match_result:
                                    tidal_track = match_result['track']
                                    confidence = match_result['confidence']
                                    batch_ids.append(tidal_track.id)
                                    imported += 1
                                    confidence_emoji = "✅" if confidence >= 0.8 else "⚠️"
                                    print(f"    {confidence_emoji} Queued: {row['name']} - {row['artist']} (confidence: {confidence:.0%})")
                                else:
                                    print(f"    ❌ Not found on TIDAL: {row['name']} - {row['artist']}")
                                    failed += 1
                                    failed_tracks.append({'track': row['name'], 'artist': row['artist'], 'playlist': playlist_name})
                                
                                if len(batch_ids) == 50:
                                    tidal_playlist.add(batch_ids)
                                    print(f"    ➡️ Added batch of 50 tracks to playlist '{playlist_name}'")
                                    batch_ids = []
                            except Exception as e:
                                print(f"    ❌ Error importing {row['name']} - {row['artist']}: {e}")
                                failed += 1
                                failed_tracks.append({'track': row['name'], 'artist': row['artist'], 'playlist': playlist_name})
                        if batch_ids:
                            tidal_playlist.add(batch_ids)
                            print(f"    ➡️ Added final batch of {len(batch_ids)} tracks to playlist '{playlist_name}'")
                # Import tracks not in any playlist as favorites
                print("\n▶️ Importing tracks not in any playlist as TIDAL favorites...")
                for idx, row in df.iterrows():
                    if playlist_col and idx in processed_idx:
                        continue
                    try:
                        # Use enhanced search with validation
                        match_result = self.search_tidal_track(
                            track_name=row['name'],
                            artist_name=row['artist'],
                            album_name=row.get('album'),
                            duration_ms=row.get('duration_ms')
                        )
                        
                        if match_result:
                            tidal_track = match_result['track']
                            confidence = match_result['confidence']
                            self.tidal.user.favorites.add_track(tidal_track.id)
                            imported += 1
                            confidence_emoji = "✅" if confidence >= 0.8 else "⚠️"
                            print(f"{confidence_emoji} Imported as favorite: {row['name']} - {row['artist']} (confidence: {confidence:.0%})")
                        else:
                            print(f"❌ Not found on TIDAL: {row['name']} - {row['artist']}")
                            failed += 1
                            failed_tracks.append({'track': row['name'], 'artist': row['artist'], 'playlist': 'Favorites'})
                    except Exception as e:
                        print(f"❌ Error importing {row['name']} - {row['artist']}: {e}")
                        failed += 1
                        failed_tracks.append({'track': row['name'], 'artist': row['artist'], 'playlist': 'Favorites'})
            except Exception as e:
                print(f"❌ Error importing tracks/playlists: {e}")
        # Import favorite artists
        if artists_csv:
            print("\n▶️ Importing followed artists as TIDAL favorites...")
            try:
                df_artists = pd.read_csv(artists_csv, encoding='utf-8')
                for _, row in df_artists.iterrows():
                    # Search for artist by name on TIDAL
                    query = row['artist_name']
                    search_result = self.tidal.search(query)
                    artists = search_result['artists'] if 'artists' in search_result else []
                    if artists:
                        tidal_artist = artists[0]
                        self.tidal.user.favorites.add_artist(tidal_artist.id)
                        print(f"⭐️ Favorited artist: {row['artist_name']}")
                    else:
                        print(f"❌ Artist not found on TIDAL: {row['artist_name']}")
            except Exception as e:
                print(f"❌ Error importing artists: {e}")
        # Import favorite albums
        if albums_csv:
            print("\n▶️ Importing saved albums as TIDAL favorites...")
            try:
                df_albums = pd.read_csv(albums_csv, encoding='utf-8')
                for _, row in df_albums.iterrows():
                    # Search for album by name and artist on TIDAL
                    query = f"{row['album_name']} {row['artist_name']}"
                    search_result = self.tidal.search(query)
                    albums = search_result['albums'] if 'albums' in search_result else []
                    if albums:
                        tidal_album = albums[0]
                        self.tidal.user.favorites.add_album(tidal_album.id)
                        print(f"💿 Favorited album: {row['album_name']} - {row['artist_name']}")
                    else:
                        print(f"❌ Album not found on TIDAL: {row['album_name']} - {row['artist_name']}")
            except Exception as e:
                print(f"❌ Error importing albums: {e}")
        
        # Summary and failed tracks report
        print(f"\n{'='*50}")
        print(f"📊 Import Summary")
        print(f"{'='*50}")
        print(f"✅ Successfully imported: {imported}")
        print(f"❌ Failed: {failed}")
        if imported + failed > 0:
            success_rate = (imported / (imported + failed)) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")
        
        # Save failed tracks to CSV for manual review
        if failed_tracks:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failed_filename = self.output_dir / f"tidal_import_failed_{timestamp}.csv"
            df_failed = pd.DataFrame(failed_tracks)
            df_failed.to_csv(failed_filename, index=False, encoding='utf-8')
            print(f"\n💾 Failed tracks saved to: {failed_filename}")
            print(f"   You can review and manually search for these tracks on TIDAL.")
    
    def list_csv_files(self) -> List[str]:
        """List available CSV files for import (only from exports directory)"""
        csv_files = []
        for file in self.output_dir.glob("*.csv"):
            csv_files.append(str(file))
        return sorted(csv_files)
    

    def run(self):
        """Main application loop"""
        while True:
            self.show_main_menu()
            choice = input("\nSelect option: ").strip().lower()

            if choice == "1a":
                self.connect_spotify()
            elif choice == "1b":
                self.connect_tidal()
            elif choice == "1c" or choice == "1":
                self.test_connections()
            elif choice == "2a":
                self.export_spotify_tracks()
            elif choice == "2b":
                # Export saved albums only
                print("\nExporting saved albums...")
                all_albums = []
                try:
                    offset = 0
                    limit = 50
                    while True:
                        results = self.spotify.current_user_saved_albums(limit=limit, offset=offset)
                        items = results['items']
                        if not items:
                            break
                        for item in items:
                            album = item['album']
                            album_data = {
                                'album_id': album['id'],
                                'album_name': album['name'],
                                'album_url': album['external_urls']['spotify'],
                                'artist_name': ', '.join([artist['name'] for artist in album['artists']])
                            }
                            all_albums.append(album_data)
                        offset += limit
                        if len(items) < limit:
                            break
                    if all_albums:
                        import pandas as pd
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = self.output_dir / f"spotify_albums_{timestamp}.csv"
                        df = pd.DataFrame(all_albums)
                        df.to_csv(filename, index=False, encoding='utf-8')
                        print(f"✅ Albums export saved to: {filename}")
                    else:
                        print("❌ No saved albums found.")
                except Exception as e:
                    print(f"❌ Error exporting saved albums: {e}")
            elif choice == "2c":
                # Export followed artists only
                print("\nExporting followed artists...")
                all_artists = []
                try:
                    after = None
                    while True:
                        results = self.spotify.current_user_followed_artists(limit=50, after=after)
                        artists = results['artists']['items']
                        if not artists:
                            break
                        for artist in artists:
                            artist_data = {
                                'artist_id': artist['id'],
                                'artist_name': artist['name'],
                                'artist_url': artist['external_urls']['spotify']
                            }
                            all_artists.append(artist_data)
                        if len(artists) < 50:
                            break
                        after = artists[-1]['id']
                    if all_artists:
                        import pandas as pd
                        from datetime import datetime
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = self.output_dir / f"spotify_artists_{timestamp}.csv"
                        df = pd.DataFrame(all_artists)
                        df.to_csv(filename, index=False, encoding='utf-8')
                        print(f"✅ Artists export saved to: {filename}")
                    else:
                        print("❌ No followed artists found.")
                except Exception as e:
                    print(f"❌ Error exporting followed artists: {e}")
            elif choice == "2d":
                self.export_spotify_playlists()
            elif choice == "2e":
                self.export_spotify_all()
            elif choice == "3":
                # Always list CSV files from exports and allow user to select one for import
                if not self.tidal:
                    if not self.connect_tidal():
                        print("❌ Could not connect to TIDAL. Import aborted.")
                        continue
                csv_files = self.list_csv_files()
                if not csv_files:
                    print("❌ No CSV files found in 'exports' directory.")
                else:
                    print("\nAvailable CSV files in 'exports':")
                    for i, f in enumerate(csv_files):
                        print(f"{i+1}. {f}")
                    file_choice = input("Select file number to import: ").strip()
                    try:
                        file_choice = int(file_choice) - 1
                        if 0 <= file_choice < len(csv_files):
                            selected_file = csv_files[file_choice]
                            import pandas as pd
                            try:
                                df = pd.read_csv(selected_file, nrows=1)
                                cols = set(df.columns.str.lower())
                                # Determine file type by columns
                                if {'name', 'artist'}.issubset(cols):
                                    # Tracks/Playlists file
                                    self.import_to_tidal(selected_file, None, None)
                                elif {'artist_name', 'artist_id'}.issubset(cols):
                                    # Artists file
                                    self.import_to_tidal(None, selected_file, None)
                                elif {'album_name', 'album_id'}.issubset(cols):
                                    # Albums file
                                    self.import_to_tidal(None, None, selected_file)
                                else:
                                    print("❌ Could not determine file type. Please select a valid export CSV.")
                            except Exception as e:
                                print(f"❌ Error reading CSV: {e}")
                        else:
                            print("❌ Invalid selection.")
                    except ValueError:
                        print("❌ Invalid input.")
            elif choice == "0":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid option. Please try again.")
            input("\nPress Enter to continue...")


def main():
    """Main entry point"""
    print("🎵 Spotify to TIDAL Transfer Tool")
    print("Built with ❤️ for seamless music migration")
    print()
    
    # Load environment variables from .env file if it exists
    env_file = Path(".env")
    if env_file.exists():
        from dotenv import load_dotenv
        load_dotenv()
    
    app = SpotifyTidalTransfer()
    app.run()


if __name__ == "__main__":
    main()
