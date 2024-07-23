import json
from pymongo import MongoClient
import os
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import quote
# import moviepy.editor as mp
# from moviepy.audio.io.AudioFileClip import AudioFileClip
from bson.binary import Binary
import uuid
# from dotenv import load_dotenv
import pymongo.errors

# Function to convert Binary data to UUID string
def convert_bin_to_uuid(bin_data: Binary) -> str:
    uuid_value = str(uuid.UUID(bytes=bytes(bin_data)))
    return uuid_value

# Function to convert UUID string to Binary data
def convert_uuid_to_bin(uuid_str: str) -> Binary:
    bin_data = Binary(uuid.UUID(uuid_str).bytes, 3)
    return bin_data

# Function to get audio URL and process it
# def get_audio_url(user_id: Binary, media_id: str, media_name: str, source_ext: str, entry: dict) -> str:
#     # Convert binary user_id to UUID format
#     user_id_str = convert_bin_to_uuid(user_id)
#
#     # Construct base S3 URL
#     base_url = "https://backsense-videos-bucket.s3.ap-south-1.amazonaws.com/"
#
#     # Construct audio file path
#     audio_file_path = f"{user_id_str}/{media_id}/original/properties/{media_name}.{source_ext}"
#
#     # Construct complete audio URL
#     audio_url = base_url + audio_file_path
#
#     # Extract original start and end times from entry
#     original_start = entry.get("source", {}).get("start", 0)
#     original_end = entry.get("source", {}).get("end", 0)
#
#     # Load audio clip
#     mp_audio_clip = AudioFileClip(audio_url)
#
#     # Trim audio clip
#     audio_segment = mp_audio_clip.subclip(original_start, original_end)
#
#     # Export trimmed audio to a local file
#     local_audio_path = f"{media_id}_{original_start}-{original_end}.wav"
#     audio_segment.write_audiofile(local_audio_path, fps=44100, codec='pcm_s16le', ffmpeg_params=["-ac", "1"], logger=None)
#
#     # Upload trimmed audio to S3
#     s3_audio_url = upload_to_s3(local_audio_path, user_id_str, media_id)
#
#     # Remove local audio file
#     os.remove(local_audio_path)
#     del audio_segment, mp_audio_clip
#     return s3_audio_url

# Function to upload file to S3
# def upload_to_s3(file_path: str, user_id: str, media_id: str) -> str:
#     # Load environment variables
#     load_dotenv()
#     AWS_STORAGE_BUCKET_NAME = os.environ["AWS_STORAGE_BUCKET_NAME"]
#     AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
#     AWS_S3_REGION_NAME = os.environ["AWS_S3_REGION_NAME"]
#     AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
#     BASE_S3_URL = os.environ["BASE_S3_URL"]
#
#     # Initialize S3 client
#     s3 = boto3.client('s3', region_name=AWS_S3_REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#
#     # Encode the filename to make it url-safe if the filename has spaces
#     encoded_file_name = quote(os.path.basename(file_path))
#     s3_path = f"/{user_id}/{media_id}/{encoded_file_name}.wav"
#     try:
#         # Upload file to S3
#         s3.upload_file(file_path, AWS_STORAGE_BUCKET_NAME, s3_path)
#         print(f"Upload of {file_path} to S3 successful.")
#     except FileNotFoundError:
#         print(f"The file {file_path} was not found.")
#     except NoCredentialsError:
#         print("Credentials not available or incorrect.")
#     except Exception as e:
#         print(f"Error uploading file to S3: {e}")
#
#     # Construct S3 URL
#     s3_url = f"{BASE_S3_URL}/{s3_path}"
#     print(s3_url)
#     return s3_url

# Function to process media by ID
def process_media_by_id(media_id: str, user_id: str, timeline_id: str):
    # MongoDB Atlas connection URI
    MONGO_URI = "mongodb+srv://backsense-user-auth:WJFRn97i4Ykpndtd@backsense-db-v3.xwbpu3k.mongodb.net/?retryWrites=true&w=majority"
    try:
        db_name = "userAuthDjango"
        # Connect to MongoDB Atlas cluster
        mongo_client = MongoClient(MONGO_URI)
        DB = mongo_client.get_database(db_name)
        print("Connected to MongoDB successfully!")
    except Exception as e:
        print(f"DB connection failed: {e}")
        raise RuntimeError("DB connection failed") from e

    # Select the collections
    media_collection = DB.get_collection("media_handler_mediamodel")

    # Find the media object by UUID
    media_object = media_collection.find_one({"_id": convert_uuid_to_bin(media_id)})

    # if media_object:
    #     if "timelines" in media_object and timeline_id in media_object["timelines"]:
    #         timelines = timelines = media_object["timelines"][timeline_id]
    #
    #         i = 0
    #         for timeline in timelines:
    #             speed = timeline["target"]["audio"]["speaker"]["speed"]
    #             # print(speed)
    #             i = i+1
    #             if speed >= 1.5 or speed <= 0.75:
    #                 print(timeline)
    #
    #         print(i)
    if media_object:
        # Check if the specified timeline exists in the media object
        if "timelines" in media_object and timeline_id in media_object["timelines"]:
            timelines = media_object["timelines"][timeline_id]
            print(f"Length of timelines array: {len(timelines)}")
            i = 0
            for entry in timelines:
                timeline_uuid = entry.get("timeline_id", {})
                print(f"Timeline ID - {timeline_uuid}")
                if timeline_uuid == 'a5d45b7c-6e4c-4e93-8951-01254d58bd10':
                    print(f"The Source Audio URL of target timeline - {entry}")
                source_audio_url = entry.get("source", {}).get("audio", {}).get("url")
                if source_audio_url:
                    # Extract UUID from the URL
                    url_parts = source_audio_url.split("/")
                    # print(f"Count - {i} URL User ID - {url_parts[3]} and User ID - {user_id}")
                    i += 1
                    # print(url_parts)
                    if len(url_parts) >= 4:
                        url_user_id = url_parts[3]
                        if url_user_id == user_id:
                            # UUID in URL matches provided user ID, continue processing
                            continue

                    # Extract and process audio
                    user_id_bin = media_object.get("user")
                    media_name = media_object.get("source", {}).get("media_name")
                    source_ext = media_object.get("source", {}).get("ext")
                    # s3_audio_url = get_audio_url(user_id_bin, media_id, media_name, source_ext, entry)
                    # Update source audio URL in the timeline entry
                    entry["source"]["audio"]["url"] = ""

                    # Update the document in the collection
                    media_collection.update_one({"_id": media_object["_id"]}, {"$set": {"timelines." + timeline_id: timelines}})
            print("Media processing completed.")
        else:
            print("Specified timeline does not exist.")
    else:
        print("Media object not found.")

# Example usage
provided_user_id = "2dce732a-f831-47ac-bdf8-39243971d45f"
provided_media_id = "49954e17-c7b1-4dbd-aed9-16665b8b180a"
provided_timeline_id = "kannada-india"
process_media_by_id(provided_media_id, provided_user_id, provided_timeline_id)