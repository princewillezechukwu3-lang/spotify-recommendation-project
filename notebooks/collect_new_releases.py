import os
import time
import random
import logging
from dotenv import load_dotenv
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
#from spotipy.exceptions import SpotifyException

load_dotenv()
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')
if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env")
if not REDIRECT_URI:
    raise SystemExit("Set SPOTIFY_REDIRECT_URI in .env")

cc = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(client_credentials_manager=cc, requests_timeout=30, retries=10)
res = sp.new_releases(country='NG', limit=1, offset=0)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("collect_new_releases")

COUNTRIES = ["US", "GB", "NG", "ZA"]
MAX_PAGES_PER_COUNTRY = 6

def get_album_ids_for_country(country):
    album_ids = []
    limit = 50

    for page in range(MAX_PAGES_PER_COUNTRY):
        offset = page * limit

        for attempt in range(5):
            try:
                res = sp.new_releases(country=country, limit=limit, offset=offset)
                break
            except Exception as e:
                logger.warning(f"Retry {attempt+1} failed for {country} offset={offset}: {e}")
                time.sleep(3)
        else:
            logger.error(f"Failed to fetch after retries: country={country}, offset={offset}")
            continue

        items = res.get("albums", {}).get("items", [])
        album_ids.extend([album["id"] for album in items])

    return album_ids

def get_tracks_from_album(album_id):
    results = []
    try:
        res = sp.album_tracks(album_id)
    except Exception as e:
        logger.warning(f"Failed to fetch album {album_id}: {e}")
        return results
    results.extend(res.get("items", []))
    while res.get("next"):
        try:
            res = sp.next(res)
            results.extend(res.get("items", []))
        except Exception:
            break
    return results

def batch_audio_features(track_ids):
    features = []
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i+100]
        try:
            af = sp.audio_features(batch)
            features.extend([a for a in af if a]) 
        except Exception as e:
            logger.warning(f"audio_features error: {e}")
            time.sleep(1)
            continue
        time.sleep(0.15)
    return features

def main():
    all_track_meta = {}
    for country in COUNTRIES:
        logger.info(f"Collecting album ids for {country}")
        album_ids = get_album_ids_for_country(country)
        logger.info(f"Found {len(album_ids)} albums for {country}")
        for aid in album_ids:
            tracks = get_tracks_from_album(aid)
            for t in tracks:
                tid = t.get("id")
                if tid and tid not in all_track_meta:
                    all_track_meta[tid] = {
                        "track_id": tid,
                        "track_name": t.get("name"),
                        "artist": t.get("artists", [{}])[0].get("name"),
                        "album_id": aid
                    }
        time.sleep(0.3 + random.random() * 0.2)

    track_ids = list(all_track_meta.keys())
    logger.info(f"Total unique track ids collected: {len(track_ids)}")


    features = batch_audio_features(track_ids)
    df_features = pd.DataFrame(features)
    df_meta = pd.DataFrame.from_dict(all_track_meta, orient="index").reset_index(drop=True)
    df = df_meta.merge(df_features, left_on="track_id", right_on="id", how="inner")
    logger.info(f"Final tracks with features: {len(df)}")

    df.to_parquet("spotify_new_releases_multi_reg.parquet", index=False)
    logger.info("Saved spotify_new_releases_multi_reg.parquet")

if __name__ == "__main__":
    main()