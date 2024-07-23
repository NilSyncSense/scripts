import csv
import pymongo
from urllib.parse import urlparse, parse_qs
from bson import Binary
import uuid
import os
import requests
from pydub import AudioSegment

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

def clip_audio(original_audio_path, clipped_audio_path, start_time, end_time):
    """
    Clips the audio segment from the original audio file based on the start and end times,
    and saves the clipped audio to the specified path.
    """
    audio = AudioSegment.from_wav(original_audio_path)
    start_time_ms = start_time * 1000  # Convert seconds to milliseconds
    end_time_ms = end_time * 1000
    clipped_audio = audio[start_time_ms:end_time_ms]
    clipped_audio.export(clipped_audio_path, format="wav")
    print(f"Audio clipped successfully: {clipped_audio_path}")

def convert_uuid_to_bin(uuid_str: str) -> Binary:
    bin_data = Binary(uuid.UUID(uuid_str).bytes, 3)
    return bin_data

def convert_bin_to_uuid(bin_data: Binary) -> str:
    uuid_value = str(uuid.UUID(bytes=bytes(bin_data)))
    return uuid_value

# MongoDB connection details
mongo_uri = "mongodb+srv://backsense-user-auth:WJFRn97i4Ykpndtd@backsense-db-v3.xwbpu3k.mongodb.net/?retryWrites=true&w=majority"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client.userAuthDjango
collection = db.media_handler_mediamodel

# Open the CSV file
with open("kareena.csv", "r") as csv_file:
    reader = csv.DictReader(csv_file)
    data = list(reader)

# Create the PSV file
with open("output.psv", "w", encoding="utf-8", newline='') as psv_file:
    psv_writer = csv.writer(psv_file, delimiter="|")
    psv_writer.writerow(["Audio clip", "Transcript", "Speaker ID"])

    for row in data:
        # Extract the media ID from the URL
        # URL - https://studio.fourie.ai/beta/edit/a466e28c-009b-4ca7-84f4-6a6626aa4d95?lang=kannada&conversion=video_to_subtitles
        url = row["Project Link"]
        parsed_url = urlparse(url)
        media_id = parsed_url.path.split("/")[-1]
        print(f"Processing begins for Media Id - {media_id}")

        # Retrieve the media object from MongoDB
        query = {'_id': convert_uuid_to_bin(str(media_id))}
        media_obj = collection.find_one(query)
        if media_obj:
            print(f"Media object fetched for Media Id - {media_id}")
            user_id = convert_bin_to_uuid(media_obj["user"])
            title = media_obj["source"]["media_name"]

            # Download the original audio
            original_audio_url = f"https://backsense-videos-bucket.s3.ap-south-1.amazonaws.com/{user_id}/{media_id}/original/properties/{title}_vocals.wav"
            original_audio_path = os.path.join("originals", f"{title}.wav")
            os.makedirs(os.path.dirname(original_audio_path), exist_ok=True)

            # Download the original audio (you need to implement the download function)
            download_audio(original_audio_url, original_audio_path)
            
            # Iterate over the timelines
            for timeline in media_obj["timelines"]["english_gl"]:
                audio_url = timeline["source"]["audio"]["url"]
                transcript = timeline["source"]["transcription"]["text"]
                speaker_id = row["Speaker"]
                start_time = timeline["source"]["start"]
                end_time = timeline["source"]["end"]

                # Extract the clipped audio segment
                clipped_audio_path = os.path.join("clips", f"{title}_{start_time}_{end_time}.wav")
                os.makedirs(os.path.dirname(clipped_audio_path), exist_ok=True)
                clip_audio(original_audio_path, clipped_audio_path, start_time, end_time)

                psv_writer.writerow([clipped_audio_path, transcript, speaker_id])
                print(f"Added for URL - {audio_url} and Speaker - {speaker_id}")


