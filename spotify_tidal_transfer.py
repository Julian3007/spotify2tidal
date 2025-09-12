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
            if not client_id or not client_secret:
                print("❌ Spotify credentials not set. Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file.")
                return False
            scope = "user-library-read playlist-read-private user-follow-read"
            auth_manager = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, scope=scope)
            self.spotify = spotipy.Spotify(auth_manager=auth_manager)
            user = self.spotify.current_user()
            print(f"✅ Connected to Spotify as {user.get('display_name', user['id'])}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Spotify: {e}")
            self.spotify = None
            return False

    def connect_tidal(self):
        """Connect to TIDAL and set self.tidal. Returns True if successful."""
        try:
            session = tidalapi.Session()
            print("🔑 Please log in to TIDAL in your browser window...")
            session.login_oauth_simple()
            self.tidal = session
            print(f"✅ Connected to TIDAL as {getattr(self.tidal.user, 'username', 'Unknown')}")
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
    
    def import_to_tidal(self, tracks_csv: str = None, artists_csv: str = None, albums_csv: str = None, mode: str = "all"):
        """Import tracks, followed artists, and albums to TIDAL from CSV files. Handles single-type files gracefully."""
        import pandas as pd
        import time
        imported = 0
        failed = 0
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
                                query = f"{row['name']} {row['artist']}"
                                search_result = self.tidal.search(query)
                                tracks = search_result['tracks'] if 'tracks' in search_result else []
                                if tracks:
                                    tidal_track = tracks[0]
                                    batch_ids.append(tidal_track.id)
                                    imported += 1
                                    print(f"    ✅ Queued: {row['name']} - {row['artist']}")
                                else:
                                    print(f"    ❌ Not found on TIDAL: {row['name']} - {row['artist']}")
                                    failed += 1
                                if len(batch_ids) == 50:
                                    tidal_playlist.add(batch_ids)
                                    print(f"    ➡️ Added batch of 50 tracks to playlist '{playlist_name}'")
                                    batch_ids = []
                            except Exception as e:
                                print(f"    ❌ Error importing {row['name']} - {row['artist']}: {e}")
                                failed += 1
                        if batch_ids:
                            tidal_playlist.add(batch_ids)
                            print(f"    ➡️ Added final batch of {len(batch_ids)} tracks to playlist '{playlist_name}'")
                # Import tracks not in any playlist as favorites
                print("\n▶️ Importing tracks not in any playlist as TIDAL favorites...")
                for idx, row in df.iterrows():
                    if playlist_col and idx in processed_idx:
                        continue
                    try:
                        query = f"{row['name']} {row['artist']}"
                        search_result = self.tidal.search(query)
                        tracks = search_result['tracks'] if 'tracks' in search_result else []
                        if tracks:
                            tidal_track = tracks[0]
                            self.tidal.user.favorites.add_track(tidal_track.id)
                            imported += 1
                            print(f"✅ Imported as favorite: {row['name']} - {row['artist']}")
                        else:
                            print(f"❌ Not found on TIDAL: {row['name']} - {row['artist']}")
                            failed += 1
                    except Exception as e:
                        print(f"❌ Error importing {row['name']} - {row['artist']}: {e}")
                        failed += 1
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
        print(f"\nImport complete. Success: {imported}, Failed: {failed}")
    
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
