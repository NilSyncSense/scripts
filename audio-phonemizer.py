import os
import shutil

from pydub import AudioSegment
import phonemizer
import tqdm
import json


def get_audio_length(file_path):
    audio = AudioSegment.from_file(file_path)
    return len(audio) / 1000


def sample(output_audio_directory,
        output_transcripts,
        json_filename,
        audio_directory,
        min_audio_len,
        max_audio_len,
        total_audio_len):
    with open(json_filename, "r", encoding='utf-8') as f:
        data = json.load(f)

    os.makedirs(output_audio_directory, exist_ok=True)

    with open(output_transcripts, "w", encoding='utf-8') as f:
        count, curr_total = 0, 0
        for item in tqdm.tqdm(data):
            sentence = item['sentence']
            try:
                phonemized_sentence = phonemizer.phonemize(sentence)
            except RuntimeError as e:
                print(f"Error phonemizing sentence: {str(e)}")
                print("Continuing with original sentence...")
                phonemized_sentence = sentence

            audio_path = os.path.join(audio_directory, item['recording'])
            if os.path.exists(audio_path):
                try:
                    audio_len = get_audio_length(audio_path)
                    if min_audio_len < audio_len < max_audio_len:
                        curr_total += audio_len
                        f.write(f"{item['recording']}|{phonemized_sentence}|{item['sid']}\n")
                        shutil.copy(audio_path, output_audio_directory)
                        count += 1
                    if curr_total / 60 > total_audio_len:
                        break
                except Exception as e:
                    print(f"Error processing {audio_path}: {str(e)}")
            else:
                print(f"Warning: Audio file not found: {audio_path}")

        print(f"Done. Sampled {count} clips totalling to {round(curr_total / 60, 2)} minutes")


if __name__ == "__main__":
    sample(
        output_audio_directory="/media/soumyanil/New Volume/syncsense/psv-file-script/V2-Submission/data-EN/output_wavs",
        output_transcripts="/media/soumyanil/New Volume/syncsense/psv-file-script/V2-Submission/data-EN/dump.txt",
        json_filename="/media/soumyanil/New Volume/syncsense/psv-file-script/V2-Submission/EN.accepted.json",
        audio_directory="/media/soumyanil/New Volume/syncsense/psv-file-script/V2-Submission/Accepted-EN",
        min_audio_len=2,
        max_audio_len=10,
        total_audio_len=100,
    )
