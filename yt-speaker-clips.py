import csv
import os
from pytube import YouTube
from moviepy.editor import VideoFileClip
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip

# Read the CSV file
with open('Asianet Data - Sheet2.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    data = list(reader)

# Get the unique video URLs
video_urls = set([row['YouTube Link'] for row in data])

# Create a directory to store the downloaded videos
if not os.path.exists('downloaded_videos'):
    os.makedirs('downloaded_videos')
if not os.path.exists('speaker_files_2'):
    os.makedirs('speaker_files_2')

speaker_data = {str(i): 0 for i in range(1, 19)}

# Process each video
for url in video_urls:
    try:
        yt = YouTube(url)
        print(f"Downloading: {yt.title}")
        video = yt.streams.filter(progressive=True, file_extension='mp4').order_by(
            'resolution').desc().first().download('downloaded_videos')
        print(f"Downloaded: {yt.title}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        continue

    # Get the video file name from the URL
    video_filename = os.path.join('downloaded_videos', f"{yt.streams.first().default_filename}")

    print(f"Processing for Filename: {video_filename}")

    # Create clips for the current video
    speaker_clips = {}
    for row in data:
        if row['YouTube Link'] == url:
            speaker_number = row['Speaker Number']
            start_time = row['Start Timestamp (min:sec)']
            end_time = row['End Timestamp (min:sec)']

            # Convert timestamps to seconds
            start_sec = sum(int(x) * 60 ** i for i, x in enumerate(reversed(start_time.split(':'))))
            end_sec = sum(int(x) * 60 ** i for i, x in enumerate(reversed(end_time.split(':'))))

            # print(start_sec)
            # print(end_sec)

            clipped_filename = os.path.join('speaker_files_2',
                                            f"Speaker {speaker_number} - Part {speaker_data[speaker_number]}.mp4")
            ffmpeg_extract_subclip(video_filename, start_sec, end_sec, targetname=clipped_filename)
            print(f"Clipped: Speaker {speaker_number} - Part {speaker_data[speaker_number]}.mp4 for URL: {url}")
            speaker_data[speaker_number] += 1

    print(f"Finsihed for Filename: {video_filename}")