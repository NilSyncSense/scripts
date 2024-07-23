import csv
import json
from pymongo import MongoClient
from bson.binary import Binary
import uuid
import os
import requests
# Function to convert UUID string to Binary data

def download_audio(url, path):
    """
    Downloads the audio file from the given URL and saves it to the specified path.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(path, "wb") as file:
            file.write(response.content)
        print(f"Audio downloaded successfully: {path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading audio: {e}")
def convert_uuid_to_bin(uuid_str: str) -> Binary:
    bin_data = Binary(uuid.UUID(uuid_str).bytes, 3)
    return bin_data

# MongoDB Atlas connection URI
MONGO_URI = "mongodb+srv://read-only-access:dzhdvIE0l7mPd3QN@backsense-db-v3.xwbpu3k.mongodb.net/"
try:
    db_name = "userAuthDjango"
    # Connect to MongoDB Atlas cluster
    mongo_client = MongoClient(MONGO_URI)
    DB = mongo_client.get_database(db_name)
    print("Connected to MongoDB successfully!")
except Exception as e:
    print(e)
    raise RuntimeError("DB connection failed") from e
# Select the collection
media_collection = DB.get_collection("media_handler_mediamodel")

# Function to extract audio URLs from the timeline
def extract_audio_urls(media_id_uuid: str):
    # Convert media_id to Binary
    media_id_bin = convert_uuid_to_bin(media_id_uuid)
    # print(media_id_bin)
    # Find the media document by media_id
    media_document = media_collection.find_one({"_id": media_id_bin})
    if not media_document:
        print("Media document not found.")
        return
    # Extract audio URLs from timeline.kannada-india
    audio_urls = []
    timeline = media_document.get("timelines", {}).get("kannada-india", [])
    # print(timeline)
    i = 1
    for entry in timeline:
        source = entry.get("source", {})
        audio_url = source.get("audio", {}).get("url")
        if audio_url:
            audio_urls.append(audio_url)
            original_audio_path = os.path.join(f"audio-clips-{media_id_uuid}", f"{i}.wav")
            i+=1
            download_audio(audio_url, original_audio_path)
    return audio_urls
# Function to save URLs to CSV and JSON
def save_urls_to_files(audio_urls, media_id_uuid):
    csv_file = f"{media_id_uuid}_audio_urls.csv"
    json_file = f"{media_id_uuid}_audio_urls.json"
    # Save to CSV
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Audio URL"])
        for url in audio_urls:
            writer.writerow([url])
    # Save to JSON
    with open(json_file, mode='w') as file:
        json.dump(audio_urls, file, indent=4)
    print(f"Audio URLs saved to {csv_file} and {json_file}")

# Example usage
media_id_uuid = '49954e17-c7b1-4dbd-aed9-16665b8b180a'
audio_urls = extract_audio_urls(media_id_uuid)
if audio_urls:
    save_urls_to_files(audio_urls, media_id_uuid)
else:
    print("No audio URLs found.")