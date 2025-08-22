#!/usr/bin/env python3
"""
Fort Collins Meeting Video Downloader
Downloads meeting videos and audio files based on the CSV database created by the scraper
"""

import pandas as pd
import requests
import os
import time
import logging
from urllib.parse import urlparse, unquote
from pathlib import Path
import re
from tqdm import tqdm
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FortCollinsVideoDownloader:
    def __init__(self, csv_file='fort_collins_meetings.csv', download_dir='downloads'):
        self.csv_file = csv_file
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for organization
        (self.download_dir / 'videos').mkdir(exist_ok=True)
        (self.download_dir / 'audio').mkdir(exist_ok=True)
        (self.download_dir / 'documents').mkdir(exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        self.downloaded_files = []
        self.failed_downloads = []
    
    def load_csv_data(self):
        """Load meeting data from CSV"""
        try:
            df = pd.read_csv(self.csv_file)
            logger.info(f"Loaded {len(df)} meetings from {self.csv_file}")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file {self.csv_file} not found. Please run the scraper first.")
            return None
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return None
    
    def sanitize_filename(self, filename):
        """Sanitize filename for safe saving"""
        # Remove or replace invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\s]+', '_', filename)  # Replace spaces with underscores
        filename = filename.strip('._')  # Remove leading/trailing dots and underscores
        return filename[:200]  # Limit length
    
    def get_filename_from_url(self, url, meeting_title, date, file_type='video'):
        """Generate appropriate filename from URL and meeting info"""
        # Parse URL to get original filename
        parsed_url = urlparse(url)
        original_name = os.path.basename(unquote(parsed_url.path))
        
        # Create meaningful filename
        clean_title = self.sanitize_filename(meeting_title)
        clean_date = self.sanitize_filename(date.replace('/', '-'))
        
        # Determine file extension
        if original_name and '.' in original_name:
            extension = os.path.splitext(original_name)[1]
        else:
            extension = '.mp4' if file_type == 'video' else '.mp3' if file_type == 'audio' else '.pdf'
        
        filename = f"{clean_date}_{clean_title}{extension}"
        return filename
    
    def download_file(self, url, local_path, chunk_size=8192):
        """Download a file with progress bar"""
        try:
            # Check if file already exists
            if local_path.exists():
                logger.info(f"File already exists: {local_path.name}")
                return True
            
            # Start download
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Get file size for progress bar
            total_size = int(response.headers.get('content-length', 0))
            
            with open(local_path, 'wb') as f:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_path.name) as pbar:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    # No content-length header, download without progress bar
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
            
            logger.info(f"Downloaded: {local_path.name} ({total_size:,} bytes)")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            # Clean up partial file
            if local_path.exists():
                local_path.unlink()
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            if local_path.exists():
                local_path.unlink()
            return False
    
    def download_meeting_files(self, meeting_row, download_videos=True, download_audio=True, download_docs=True):
        """Download all files for a specific meeting"""
        title = meeting_row['title']
        date = meeting_row['date']
        
        logger.info(f"Processing: {title} ({date})")
        
        files_downloaded = []
        
        # Download video files
        if download_videos and pd.notna(meeting_row.get('video_link', '')) and meeting_row['video_link']:
            video_url = meeting_row['video_link']
            filename = self.get_filename_from_url(video_url, title, date, 'video')
            video_path = self.download_dir / 'videos' / filename
            
            if self.download_file(video_url, video_path):
                files_downloaded.append(str(video_path))
            else:
                self.failed_downloads.append({'url': video_url, 'type': 'video', 'meeting': title})
        
        # Download MP4 downloads (if different from video_link)
        if download_videos and pd.notna(meeting_row.get('mp4_download', '')) and meeting_row['mp4_download']:
            mp4_url = meeting_row['mp4_download']
            if mp4_url != meeting_row.get('video_link', ''):  # Avoid duplicates
                filename = self.get_filename_from_url(mp4_url, title, date, 'video')
                mp4_path = self.download_dir / 'videos' / filename
                
                if self.download_file(mp4_url, mp4_path):
                    files_downloaded.append(str(mp4_path))
                else:
                    self.failed_downloads.append({'url': mp4_url, 'type': 'mp4', 'meeting': title})
        
        # Download audio files
        if download_audio and pd.notna(meeting_row.get('audio_link', '')) and meeting_row['audio_link']:
            audio_url = meeting_row['audio_link']
            filename = self.get_filename_from_url(audio_url, title, date, 'audio')
            audio_path = self.download_dir / 'audio' / filename
            
            if self.download_file(audio_url, audio_path):
                files_downloaded.append(str(audio_path))
            else:
                self.failed_downloads.append({'url': audio_url, 'type': 'audio', 'meeting': title})
        
        # Download documents (PDFs)
        if download_docs:
            # Agenda PDF
            if pd.notna(meeting_row.get('agenda_pdf', '')) and meeting_row['agenda_pdf']:
                agenda_url = meeting_row['agenda_pdf']
                filename = self.get_filename_from_url(agenda_url, f"{title}_agenda", date, 'doc')
                doc_path = self.download_dir / 'documents' / filename
                
                if self.download_file(agenda_url, doc_path):
                    files_downloaded.append(str(doc_path))
                else:
                    self.failed_downloads.append({'url': agenda_url, 'type': 'agenda_pdf', 'meeting': title})
            
            # Minutes PDF
            if pd.notna(meeting_row.get('minutes_pdf', '')) and meeting_row['minutes_pdf']:
                minutes_url = meeting_row['minutes_pdf']
                filename = self.get_filename_from_url(minutes_url, f"{title}_minutes", date, 'doc')
                doc_path = self.download_dir / 'documents' / filename
                
                if self.download_file(minutes_url, doc_path):
                    files_downloaded.append(str(doc_path))
                else:
                    self.failed_downloads.append({'url': minutes_url, 'type': 'minutes_pdf', 'meeting': title})
        
        if files_downloaded:
            self.downloaded_files.extend(files_downloaded)
            logger.info(f"Downloaded {len(files_downloaded)} files for meeting: {title}")
        else:
            logger.warning(f"No files downloaded for meeting: {title}")
        
        # Rate limiting
        time.sleep(1)
    
    def filter_meetings(self, df, meeting_types=None, date_range=None, limit=None):
        """Filter meetings based on criteria"""
        filtered_df = df.copy()
        
        # Filter by meeting type
        if meeting_types:
            filtered_df = filtered_df[filtered_df['meeting_type'].isin(meeting_types)]
            logger.info(f"Filtered to {len(filtered_df)} meetings by type: {meeting_types}")
        
        # Filter by date range
        if date_range:
            try:
                start_date, end_date = date_range
                filtered_df['date_parsed'] = pd.to_datetime(filtered_df['date'], errors='coerce')
                filtered_df = filtered_df[
                    (filtered_df['date_parsed'] >= start_date) & 
                    (filtered_df['date_parsed'] <= end_date)
                ]
                filtered_df = filtered_df.drop('date_parsed', axis=1)
                logger.info(f"Filtered to {len(filtered_df)} meetings by date range: {start_date} to {end_date}")
            except Exception as e:
                logger.warning(f"Date filtering failed: {e}")
        
        # Limit number of meetings
        if limit:
            filtered_df = filtered_df.head(limit)
            logger.info(f"Limited to {len(filtered_df)} meetings")
        
        return filtered_df
    
    def download_all(self, meeting_types=None, date_range=None, limit=None, 
                    download_videos=True, download_audio=True, download_docs=True):
        """Download files for all meetings matching criteria"""
        # Load data
        df = self.load_csv_data()
        if df is None:
            return
        
        # Apply filters
        filtered_df = self.filter_meetings(df, meeting_types, date_range, limit)
        
        if len(filtered_df) == 0:
            logger.warning("No meetings match the specified criteria")
            return
        
        logger.info(f"Starting download of files for {len(filtered_df)} meetings...")
        
        # Download files for each meeting
        for index, meeting in filtered_df.iterrows():
            try:
                self.download_meeting_files(meeting, download_videos, download_audio, download_docs)
            except KeyboardInterrupt:
                logger.info("Download interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error processing meeting {meeting['title']}: {e}")
                continue
        
        # Print summary
        self.print_summary()
    
    def print_summary(self):
        """Print download summary"""
        print(f"\n=== DOWNLOAD SUMMARY ===")
        print(f"Successfully downloaded: {len(self.downloaded_files)} files")
        print(f"Failed downloads: {len(self.failed_downloads)}")
        
        if self.downloaded_files:
            print(f"\nDownloaded files saved to: {self.download_dir}")
            
        if self.failed_downloads:
            print(f"\nFailed downloads:")
            for failed in self.failed_downloads:
                print(f"  - {failed['type']}: {failed['meeting']} ({failed['url']})")

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Download Fort Collins meeting videos and documents')
    parser.add_argument('--csv', default='fort_collins_meetings.csv', help='CSV file with meeting data')
    parser.add_argument('--output', default='downloads', help='Output directory for downloads')
    parser.add_argument('--types', nargs='+', help='Meeting types to download (e.g., "City Council Regular Meeting")')
    parser.add_argument('--limit', type=int, help='Limit number of meetings to process')
    parser.add_argument('--no-videos', action='store_true', help='Skip video downloads')
    parser.add_argument('--no-audio', action='store_true', help='Skip audio downloads')
    parser.add_argument('--no-docs', action='store_true', help='Skip document downloads')
    
    args = parser.parse_args()
    
    try:
        downloader = FortCollinsVideoDownloader(args.csv, args.output)
        downloader.download_all(
            meeting_types=args.types,
            limit=args.limit,
            download_videos=not args.no_videos,
            download_audio=not args.no_audio,
            download_docs=not args.no_docs
        )
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Download failed: {e}")

if __name__ == "__main__":
    main()
