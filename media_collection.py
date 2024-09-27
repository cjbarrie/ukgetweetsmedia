import os
import pandas as pd
import requests
import logging
import hashlib
import bs4
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(os.path.join(log_dir, "media_collection.log")),
                              logging.StreamHandler()])

def hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()

# Image download function
def download_image(image_url, image_hash, download_dir):
    """Download a single image if it hasn't already been downloaded."""
    file_name = f"{image_hash}.jpg"
    file_path = os.path.join(download_dir, file_name)
    
    # Check if the file already exists
    if os.path.exists(file_path):
        logging.info(f"Image already exists: {file_path}. Skipping download.")
        return

    try:
        response = requests.get(image_url)
        response.raise_for_status()

        # Save the image content
        with open(file_path, 'wb') as file:
            file.write(response.content)

        logging.info(f"Downloaded and saved image to: {file_path}")
    except requests.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logging.error(f"An error occurred: {err}")

def download_images(csv_path, download_dir):
    """Downloads images from the specified CSV to the download directory concurrently."""
    media_data = pd.read_csv(csv_path)
    unique_images = media_data.dropna(subset=['image_url']).drop_duplicates(subset=['image_url'])
    
    # Ensure directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(download_image, row['image_url'], row['image_hash'], download_dir)
            for _, row in unique_images.iterrows()
        ]
        for future in as_completed(futures):
            future.result()  # Trigger any exceptions

# Video URL extraction function
def extract_highest_quality_video_url(api_url):
    """Extract the highest quality video URL using web scraping."""
    response = requests.get(api_url)
    data = bs4.BeautifulSoup(response.text, "html.parser")
    download_button = data.find_all("div", class_="origin-top-right")[0]
    quality_buttons = download_button.find_all("a")
    highest_quality_url = quality_buttons[0].get("href")  # Assuming the first link is the highest quality
    return highest_quality_url

# Video download function
def download_video(url, video_hash, download_dir):
    """Download a video from a URL into a file path if it hasn't already been downloaded."""
    file_name = f"{video_hash}.mp4"
    file_path = os.path.join(download_dir, file_name)

    # Check if the file already exists
    if os.path.exists(file_path):
        logging.info(f"Video already exists: {file_path}. Skipping download.")
        return

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024
        progress_bar = tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {file_path}")

        with open(file_path, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)

        progress_bar.close()
        logging.info(f"Video downloaded successfully and saved to: {file_path}")
    except requests.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logging.error(f"An error occurred: {err}")

def download_videos(csv_path, download_dir):
    """Downloads videos from the specified CSV to the download directory concurrently."""
    media_data = pd.read_csv(csv_path)
    unique_videos = media_data.dropna(subset=['video_url']).drop_duplicates(subset=['video_url'])
    
    # Ensure directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Generate video hashes
    unique_videos['video_hash'] = unique_videos['video_url'].apply(lambda x: hash_url(x) if pd.notna(x) else None)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for _, row in unique_videos.iterrows():
            video_url = row['video_url']
            video_hash = row['video_hash']

            # Check if the file already exists
            file_name = f"{video_hash}.mp4"
            file_path = os.path.join(download_dir, file_name)
            if os.path.exists(file_path):
                logging.info(f"Video already exists: {file_path}. Skipping download.")
                continue

            try:
                highest_quality_url = extract_highest_quality_video_url(f"https://twitsave.com/info?url={video_url}")
                futures.append(executor.submit(download_video, highest_quality_url, video_hash, download_dir))
            except Exception as err:
                logging.error(f"An error occurred while extracting video URL: {err}")
        
        for future in as_completed(futures):
            future.result()  # Trigger any exceptions

# Main function to run the full download
def run_media_collection():
    # Paths to CSV files
    csv_path_images = 'data/processed/mpmediatweets_all_hashed.csv'
    csv_path_videos = 'data/processed/sampled_videos.csv'
    
    # Relative paths for saving media files
    # Uncomment these lines for HPC collection
    # image_save_dir = 'collected/images'
    # video_save_dir = 'collected/videos'
    
    # Uncomment these lines for local collection on the external drive
    image_save_dir = '/Volumes/T7/ukgetweets2/collected/images'
    video_save_dir = '/Volumes/T7/ukgetweets2/collected/videos'
    
    # Ensure directories exist
    if not os.path.exists(image_save_dir):
        os.makedirs(image_save_dir)
    if not os.path.exists(video_save_dir):
        os.makedirs(video_save_dir)

    with ThreadPoolExecutor() as executor:
        # Run image and video downloads concurrently
        futures = [
            executor.submit(download_images, csv_path_images, image_save_dir),
            executor.submit(download_videos, csv_path_videos, video_save_dir)
        ]

        for future in as_completed(futures):
            future.result()  # Trigger any exceptions

    logging.info("Media collection process completed.")

# Run the main collection process
if __name__ == "__main__":
    run_media_collection()
