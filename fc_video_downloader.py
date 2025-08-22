#!/usr/bin/env python3
"""
Extended Fort Collins Meeting Video and Document Downloader

This module builds upon the original ``fc_video_downloader.py`` from the
``Miata999/foco_webscrape`` repository.  In addition to videos, audio and
PDF documents, the enhanced downloader can fetch text transcripts when
available.  These transcripts are produced by the Cablecast platform and
are delivered as ``transcript.en.txt`` files alongside the MP4 assets.

## New Features

* **Transcript downloads** – If a meeting row contains a ``transcript_url``
  field (as produced by the updated scraper), the downloader will
  download the transcript into the ``documents`` subdirectory.  The
  transcript is treated as its own file type (``transcript``) for
  archive‑tracking purposes.

* **Expanded meeting type filtering** – Because the scraper now captures
  Historic Preservation and Planning & Zoning meetings, you can pass
  their names via ``--types`` to limit downloads accordingly.  For
  example:

  ``python fc_video_downloader_updated.py --types "Historic Preservation Commission Regular Meeting" "Planning & Zoning Commission Regular Meeting"``

All original command‑line options and behaviors remain unchanged.  The
only difference is that transcripts are downloaded alongside other
documents by default.
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
import json
import hashlib
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedFortCollinsVideoDownloader:
    """Download videos, audio, documents and transcripts based on meeting CSV."""
    def __init__(self, csv_file: str = 'fort_collins_meetings.csv', download_dir: str = 'downloads') -> None:
        self.csv_file = csv_file
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        # Subdirectories
        (self.download_dir / 'videos').mkdir(exist_ok=True)
        (self.download_dir / 'audio').mkdir(exist_ok=True)
        (self.download_dir / 'documents').mkdir(exist_ok=True)
        # Archive tracking
        self.archive_file = self.download_dir / 'download_archive.json'
        self.downloaded_files: list[dict] = []
        self.failed_downloads: list[dict] = []
        self.archive_data = self.load_archive()
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def load_archive(self) -> dict:
        """Load existing archive or initialize a new one."""
        if self.archive_file.exists():
            try:
                with open(self.archive_file, 'r') as f:
                    archive = json.load(f)
                logger.info(f"Loaded archive with {len(archive.get('downloads', []))} tracked files")
                return archive
            except Exception as e:
                logger.warning(f"Error loading archive: {e}")
                return {'downloads': [], 'last_updated': None}
        logger.info("No existing archive found, starting fresh")
        return {'downloads': [], 'last_updated': None}

    def save_archive(self) -> None:
        """Persist archive to disk."""
        self.archive_data['last_updated'] = datetime.now().isoformat()
        try:
            with open(self.archive_file, 'w') as f:
                json.dump(self.archive_data, f, indent=2)
            logger.info(f"Archive saved with {len(self.archive_data['downloads'])} tracked files")
        except Exception as e:
            logger.error(f"Error saving archive: {e}")

    def generate_file_hash(self, url: str, meeting_title: str, date: str, file_type: str) -> str:
        """Generate a unique hash for a file based on its metadata."""
        identifier = f"{url}_{meeting_title}_{date}_{file_type}"
        return hashlib.md5(identifier.encode()).hexdigest()

    def is_file_downloaded(self, url: str, meeting_title: str, date: str, file_type: str) -> tuple[bool, Path | None]:
        """Check if a file has already been downloaded."""
        file_hash = self.generate_file_hash(url, meeting_title, date, file_type)
        for download_record in self.archive_data['downloads']:
            if download_record.get('file_hash') == file_hash:
                file_path = Path(download_record.get('file_path', ''))
                if file_path.exists():
                    logger.debug(f"File already downloaded: {file_path.name}")
                    return True, file_path
                else:
                    logger.warning(f"Archived file not found on disk: {file_path}")
                    self.archive_data['downloads'] = [d for d in self.archive_data['downloads'] if d.get('file_hash') != file_hash]
        return False, None

    def add_to_archive(self, url: str, meeting_title: str, date: str, file_type: str, file_path: Path, file_size: int) -> None:
        """Record a successfully downloaded file in the archive."""
        file_hash = self.generate_file_hash(url, meeting_title, date, file_type)
        download_record = {
            'file_hash': file_hash,
            'url': url,
            'meeting_title': meeting_title,
            'date': date,
            'file_type': file_type,
            'file_path': str(file_path),
            'file_size': file_size,
            'downloaded_at': datetime.now().isoformat(),
            'filename': file_path.name
        }
        self.archive_data['downloads'] = [d for d in self.archive_data['downloads'] if d.get('file_hash') != file_hash]
        self.archive_data['downloads'].append(download_record)

    def load_csv_data(self) -> pd.DataFrame | None:
        """Load meeting metadata from CSV."""
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

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize a filename for saving to disk."""
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\s]+', '_', filename)
        return filename.strip('._')[:200]

    def get_filename_from_url(self, url: str, meeting_title: str, date: str, file_type: str = 'video') -> str:
        """Generate a friendly filename based on meeting info and URL."""
        parsed_url = urlparse(url)
        original_name = os.path.basename(unquote(parsed_url.path))
        clean_title = self.sanitize_filename(meeting_title)
        clean_date = self.sanitize_filename(date.replace('/', '-'))
        if original_name and '.' in original_name:
            extension = os.path.splitext(original_name)[1]
        else:
            # Default extensions by file type
            if file_type == 'video':
                extension = '.mp4'
            elif file_type == 'audio':
                extension = '.mp3'
            elif file_type == 'transcript':
                extension = '.txt'
            else:
                extension = '.pdf'
        return f"{clean_date}_{clean_title}{extension}"

    def download_file(self, url: str, local_path: Path, chunk_size: int = 8192) -> int:
        """Download a file from a URL with a progress bar.  Returns the number of bytes saved."""
        try:
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            bytes_written = 0
            with open(local_path, 'wb') as f:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_path.name) as pbar:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                bytes_written += len(chunk)
                                pbar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)
            return bytes_written
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return 0

    def filter_meetings(self, df: pd.DataFrame, meeting_types: list[str] | None = None, date_range: tuple | None = None, limit: int | None = None) -> pd.DataFrame:
        """Filter the dataframe by meeting types, date range and limit."""
        filtered_df = df.copy()
        if meeting_types:
            filtered_df = filtered_df[filtered_df['meeting_type'].isin(meeting_types)]
            logger.info(f"Filtered to {len(filtered_df)} meetings by type: {meeting_types}")
        if date_range:
            try:
                start_date, end_date = date_range
                filtered_df['date_parsed'] = pd.to_datetime(filtered_df['date'], errors='coerce')
                filtered_df = filtered_df[(filtered_df['date_parsed'] >= start_date) & (filtered_df['date_parsed'] <= end_date)]
                filtered_df = filtered_df.drop('date_parsed', axis=1)
                logger.info(f"Filtered to {len(filtered_df)} meetings by date range: {start_date} to {end_date}")
            except Exception as e:
                logger.warning(f"Date filtering failed: {e}")
        if limit:
            filtered_df = filtered_df.head(limit)
            logger.info(f"Limited to {len(filtered_df)} meetings")
        return filtered_df

    def download_meeting_files(self, meeting: pd.Series, download_videos: bool, download_audio: bool, download_docs: bool) -> None:
        """Download available files for a single meeting row."""
        title = meeting.get('title', '')
        date = meeting.get('date', '')
        # Video
        if download_videos:
            video_url = meeting.get('mp4_download') or meeting.get('video_link')
            if video_url:
                already, existing = self.is_file_downloaded(video_url, title, date, 'video')
                if not already:
                    filename = self.get_filename_from_url(video_url, title, date, 'video')
                    local_path = self.download_dir / 'videos' / filename
                    logger.info(f"Downloading video: {title} ({date})")
                    size = self.download_file(video_url, local_path)
                    if size > 0:
                        self.add_to_archive(video_url, title, date, 'video', local_path, size)
                        self.downloaded_files.append({'type': 'video', 'meeting': title, 'url': video_url})
                    else:
                        self.failed_downloads.append({'type': 'video', 'meeting': title, 'url': video_url})
        # Audio
        if download_audio:
            audio_url = meeting.get('audio_link')
            if audio_url:
                already, existing = self.is_file_downloaded(audio_url, title, date, 'audio')
                if not already:
                    filename = self.get_filename_from_url(audio_url, title, date, 'audio')
                    local_path = self.download_dir / 'audio' / filename
                    logger.info(f"Downloading audio: {title} ({date})")
                    size = self.download_file(audio_url, local_path)
                    if size > 0:
                        self.add_to_archive(audio_url, title, date, 'audio', local_path, size)
                        self.downloaded_files.append({'type': 'audio', 'meeting': title, 'url': audio_url})
                    else:
                        self.failed_downloads.append({'type': 'audio', 'meeting': title, 'url': audio_url})
        # Documents (PDF agendas, minutes, transcripts)
        if download_docs:
            doc_links = []
            for field_name in ['agenda_pdf', 'agenda_html', 'minutes_pdf', 'minutes_html']:
                link = meeting.get(field_name)
                if isinstance(link, str) and link:
                    doc_links.append((link, 'document'))
            # Transcript
            transcript_url = meeting.get('transcript_url')
            if isinstance(transcript_url, str) and transcript_url:
                doc_links.append((transcript_url, 'transcript'))
            for url, file_type in doc_links:
                already, existing = self.is_file_downloaded(url, title, date, file_type)
                if already:
                    continue
                filename = self.get_filename_from_url(url, title, date, file_type)
                local_path = self.download_dir / 'documents' / filename
                logger.info(f"Downloading {file_type}: {title} ({date})")
                size = self.download_file(url, local_path)
                if size > 0:
                    self.add_to_archive(url, title, date, file_type, local_path, size)
                    self.downloaded_files.append({'type': file_type, 'meeting': title, 'url': url})
                else:
                    self.failed_downloads.append({'type': file_type, 'meeting': title, 'url': url})

    def download_all(self, meeting_types: list[str] | None = None, date_range: tuple | None = None, limit: int | None = None,
                     download_videos: bool = True, download_audio: bool = True, download_docs: bool = True) -> None:
        """Download all requested files based on filters."""
        df = self.load_csv_data()
        if df is None:
            return
        filtered_df = self.filter_meetings(df, meeting_types, date_range, limit)
        if len(filtered_df) == 0:
            logger.warning("No meetings match the specified criteria")
            return
        logger.info(f"Starting download of files for {len(filtered_df)} meetings...")
        logger.info(f"Archive contains {len(self.archive_data['downloads'])} previously downloaded files")
        for _, meeting in filtered_df.iterrows():
            try:
                self.download_meeting_files(meeting, download_videos, download_audio, download_docs)
            except KeyboardInterrupt:
                logger.info("Download interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error processing meeting {meeting.get('title', '')}: {e}")
                continue
        self.save_archive()
        self.print_summary()

    def print_summary(self) -> None:
        """Print a summary of the current download session."""
        print("\n=== ENHANCED DOWNLOAD SUMMARY ===")
        print(f"Successfully downloaded: {len(self.downloaded_files)} new files")
        print(f"Failed downloads: {len(self.failed_downloads)}")
        print(f"Total files in archive: {len(self.archive_data['downloads'])}")
        if self.downloaded_files:
            print(f"\nNew files saved to: {self.download_dir}")
        if self.failed_downloads:
            print("\nFailed downloads:")
            for failed in self.failed_downloads:
                print(f"  - {failed['type']}: {failed['meeting']} ({failed['url']})")
        # Archive statistics
        if self.archive_data['downloads']:
            total_size = sum(d.get('file_size', 0) for d in self.archive_data['downloads'])
            print(f"\nArchive statistics:")
            print(f"  Total files: {len(self.archive_data['downloads'])}")
            print(f"  Total size: {total_size:,} bytes ({total_size / (1024**3):.2f} GB)")
            # File type breakdown
            file_types: dict[str, int] = {}
            for download in self.archive_data['downloads']:
                ftype = download.get('file_type', 'unknown')
                file_types[ftype] = file_types.get(ftype, 0) + 1
            print(f"  File types:")
            for ftype, count in file_types.items():
                print(f"    {ftype}: {count}")


def main() -> None:
    """Command line interface for the enhanced downloader."""
    parser = argparse.ArgumentParser(description='Enhanced Fort Collins meeting video, audio, document and transcript downloader')
    parser.add_argument('--csv', default='fort_collins_all_meetings.csv', help='CSV file with meeting data (default: fort_collins_all_meetings.csv)')
    parser.add_argument('--output', default='downloads', help='Output directory for downloads (default: downloads)')
    parser.add_argument('--types', nargs='+', help='Meeting types to download (e.g., "Historic Preservation Commission Regular Meeting")')
    parser.add_argument('--limit', type=int, help='Limit number of meetings to process')
    parser.add_argument('--no-videos', action='store_true', help='Skip video downloads')
    parser.add_argument('--no-audio', action='store_true', help='Skip audio downloads')
    parser.add_argument('--no-docs', action='store_true', help='Skip document and transcript downloads')
    parser.add_argument('--show-archive', action='store_true', help='Show archive statistics and exit')
    args = parser.parse_args()
    try:
        downloader = EnhancedFortCollinsVideoDownloader(args.csv, args.output)
        if args.show_archive:
            print("=== DOWNLOAD ARCHIVE ===")
            print(f"Archive file: {downloader.archive_file}")
            print(f"Total tracked files: {len(downloader.archive_data['downloads'])}")
            if downloader.archive_data['last_updated']:
                print(f"Last updated: {downloader.archive_data['last_updated']}")
            if downloader.archive_data['downloads']:
                total_size = sum(d.get('file_size', 0) for d in downloader.archive_data['downloads'])
                print(f"Total size: {total_size:,} bytes ({total_size / (1024**3):.2f} GB)")
                print("\nRecent downloads:")
                recent_downloads = sorted(downloader.archive_data['downloads'], key=lambda x: x.get('downloaded_at', ''), reverse=True)[:10]
                for download in recent_downloads:
                    print(f"  {download.get('downloaded_at', '')[:10]} - {download.get('filename', '')}")
            return
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


if __name__ == '__main__':
    main()