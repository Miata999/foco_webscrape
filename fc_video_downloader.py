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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedFortCollinsVideoDownloader:
    """Download videos, audio, documents and transcripts based on meeting CSV."""
    def __init__(self, csv_file: str = 'fort_collins_meetings.csv', download_dir: str = 'downloads', max_workers: int = 5) -> None:
        self.csv_file = csv_file
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        # Subdirectories
        (self.download_dir / 'videos').mkdir(exist_ok=True)
        (self.download_dir / 'audio').mkdir(exist_ok=True)
        (self.download_dir / 'documents').mkdir(exist_ok=True)
        # Archive tracking
        self.archive_file = self.download_dir / 'download_archive.json'
        self.failed_downloads_file = self.download_dir / 'failed_downloads.json'
        self.downloaded_files: list[dict] = []
        self.failed_downloads: list[dict] = []
        self.archive_data = self.load_archive()
        self.archive_lock = threading.Lock()
        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.max_workers = max_workers

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
        with self.archive_lock:
            self.archive_data['last_updated'] = datetime.now().isoformat()
            try:
                with open(self.archive_file, 'w') as f:
                    json.dump(self.archive_data, f, indent=2)
                logger.info(f"Archive saved with {len(self.archive_data['downloads'])} tracked files")
            except Exception as e:
                logger.error(f"Error saving archive: {e}")

    def load_failed_downloads(self) -> list:
        """Load the list of failed downloads."""
        if self.failed_downloads_file.exists():
            try:
                with open(self.failed_downloads_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading failed downloads file: {e}")
        return []

    def save_failed_downloads(self) -> None:
        """Save the list of failed downloads."""
        try:
            with open(self.failed_downloads_file, 'w') as f:
                json.dump(self.failed_downloads, f, indent=2)
            logger.info(f"Saved {len(self.failed_downloads)} failed downloads to {self.failed_downloads_file}")
        except Exception as e:
            logger.error(f"Error saving failed downloads: {e}")

    def generate_file_hash(self, url, meeting_title, date, file_type):
        """Generate a unique hash for a file based on its metadata."""
        identifier = f"{url}_{meeting_title}_{date}_{file_type}"
        return hashlib.md5(identifier.encode()).hexdigest()

    def is_file_downloaded(self, url, meeting_title, date, file_type):
        """Check if a file has already been downloaded."""
        file_hash = self.generate_file_hash(url, meeting_title, date, file_type)
        with self.archive_lock:
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

    def add_to_archive(self, url, meeting_title, date, file_type, file_path, file_size):
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
        with self.archive_lock:
            self.archive_data['downloads'] = [d for d in self.archive_data['downloads'] if d.get('file_hash') != file_hash]
            self.archive_data['downloads'].append(download_record)

    def load_csv_data(self):
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

    def sanitize_filename(self, filename):
        """Sanitize a filename for saving to disk."""
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\s]+', '_', filename)
        return filename.strip('._')[:200]

    def get_filename_from_url(self, url, meeting_title, date, file_type='video'):
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

    def download_file(self, url, local_path, chunk_size=8192):
        """Download a file from a URL with a progress bar.  Returns the number of bytes saved."""
        try:
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            bytes_written = 0
            with open(local_path, 'wb') as f:
                if total_size > 0:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=local_path.name, leave=False) as pbar:
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
            if local_path.exists():
                os.remove(local_path)
            return 0

    def _extract_media_urls_from_html(self, soup: BeautifulSoup, base_url: str) -> dict:
        """Extract media URLs from common HTML elements and script text."""
        media = {'mp4': [], 'mpeg': [], 'm3u8': []}

        def add(kind: str, url_val: str):
            if not url_val:
                return
            if not url_val.startswith('http'):
                url_val = requests.compat.urljoin(base_url, url_val)
            if url_val not in media[kind]:
                media[kind].append(url_val)

        for tag_name, attr in [('a', 'href'), ('source', 'src'), ('video', 'src'), ('link', 'href')]:
            for tag in soup.find_all(tag_name):
                val = tag.get(attr, '')
                if not val:
                    continue
                low = val.lower()
                if '.mp4' in low:
                    add('mp4', val)
                if '.mpeg' in low:
                    add('mpeg', val)
                if '.m3u8' in low:
                    add('m3u8', val)

        combined_script = '\n'.join([s.get_text(' ', strip=False) for s in soup.find_all('script')])
        if combined_script:
            for pattern, kind in [
                (r'https?://[^"\'\s>]+\.mp4[^"\'\s>]*', 'mp4'),
                (r'https?://[^"\'\s>]+\.mpeg[^"\'\s>]*', 'mpeg'),
                (r'https?://[^"\'\s>]+\.m3u8[^"\'\s>]*', 'm3u8'),
            ]:
                try:
                    for match in re.findall(pattern, combined_script, flags=re.IGNORECASE):
                        add(kind, match)
                except re.error:
                    continue

        return media

    def _resolve_cablecast_show_to_mp4(self, page_url: str) -> str:
        """Fetch a Cablecast show page and try to resolve a direct MP4 link."""
        try:
            resp = self.session.get(page_url, timeout=20)
            if resp.status_code != 200:
                return ''
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Simple anchor first
            for a in soup.find_all('a', href=True):
                href = a['href']
                if '.mp4' in href.lower():
                    return href if href.startswith('http') else requests.compat.urljoin(page_url, href)
            # Broader scan
            media = self._extract_media_urls_from_html(soup, page_url)
            if media.get('mp4'):
                return media['mp4'][0]
            if media.get('mpeg'):
                return media['mpeg'][0]
            # Try deriving MP4 from HLS if present
            if media.get('m3u8'):
                candidate = media['m3u8'][0].split('?', 1)[0].rsplit('.', 1)[0] + '.mp4'
                try:
                    head = self.session.head(candidate, timeout=10, allow_redirects=True)
                    if head.status_code == 200 and int(head.headers.get('content-length', '0')) > 0:
                        return candidate
                except Exception:
                    pass
            # Try common embed endpoints using show ID
            show_id = None
            m = re.search(r'/show/(\d+)', page_url)
            if m:
                show_id = m.group(1)
            if show_id:
                embed_candidates = [
                    f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/resource/embed/iframe?show={show_id}&site=1",
                    f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/embed?show={show_id}&site=1",
                    f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/resource/embed/iframe?show={show_id}&site=1",
                ]
                for embed_url in embed_candidates:
                    try:
                        eresp = self.session.get(embed_url, timeout=15)
                        if eresp.status_code != 200:
                            continue
                        esoup = BeautifulSoup(eresp.content, 'html.parser')
                        emedia = self._extract_media_urls_from_html(esoup, embed_url)
                        if emedia.get('mp4'):
                            return emedia['mp4'][0]
                        if emedia.get('mpeg'):
                            return emedia['mpeg'][0]
                        if emedia.get('m3u8'):
                            candidate2 = emedia['m3u8'][0].split('?', 1)[0].rsplit('.', 1)[0] + '.mp4'
                            try:
                                head2 = self.session.head(candidate2, timeout=10, allow_redirects=True)
                                if head2.status_code == 200 and int(head2.headers.get('content-length', '0')) > 0:
                                    return candidate2
                            except Exception:
                                pass
                    except Exception:
                        continue
            # Text fallback
            text = soup.get_text(' ')
            m = re.search(r'https?://[^\s"\'>]+\.mp4[^\s"\'>]*', text, flags=re.IGNORECASE)
            return m.group(0) if m else ''
        except Exception as e:
            logger.debug(f"Failed to resolve MP4 from {page_url}: {e}")
            return ''

    def _download_worker(self, meeting, download_videos, download_audio, download_docs):
        """Worker function to download files for a single meeting."""
        try:
            return self.download_meeting_files(meeting, download_videos, download_audio, download_docs)
        except Exception as e:
            logger.error(f"Error in download worker for meeting {meeting.get('title', '')}: {e}")
            return [], [{'type': 'unknown', 'meeting': meeting.get('title', ''), 'url': '', 'error': str(e)}]

    def download_meeting_files(self, meeting, download_videos, download_audio, download_docs):
        """Download available files for a single meeting row."""
        title = meeting.get('title', '')
        date = meeting.get('date', '')
        downloaded = []
        failed = []

        # Video
        if download_videos:
            video_url = meeting.get('mp4_download') or meeting.get('video_link')
            if video_url and pd.notna(video_url) and str(video_url).strip():
                # If it's a Cablecast show page, attempt to resolve a direct MP4 first
                url_str = str(video_url)
                if 'cablecast.tv' in url_str and '/show/' in url_str:
                    resolved = self._resolve_cablecast_show_to_mp4(url_str)
                    if resolved:
                        video_url = resolved
                # If the resolved URL looks unrelated (e.g., same file across many rows), prefer IDs in path
                try:
                    parsed = requests.utils.urlparse(str(video_url))
                    # Extract show id if present in meeting row
                    meeting_id = None
                    if 'video_id' in meeting and pd.notna(meeting['video_id']):
                        meeting_id = str(int(float(meeting['video_id'])))
                    if meeting_id and parsed.path:
                        if meeting_id not in parsed.path:
                            # Attempt to discover a better candidate via embed fetch
                            better = self._resolve_cablecast_show_to_mp4(meeting.get('video_link') or '')
                            if better:
                                video_url = better
                except Exception:
                    pass
                already, _ = self.is_file_downloaded(video_url, title, date, 'video')
                if not already:
                    filename = self.get_filename_from_url(video_url, title, date, 'video')
                    local_path = self.download_dir / 'videos' / filename
                    logger.info(f"Downloading video: {title} ({date})")
                    size = self.download_file(video_url, local_path)
                    if size > 0:
                        self.add_to_archive(video_url, title, date, 'video', local_path, size)
                        downloaded.append({'type': 'video', 'meeting': title, 'url': video_url})
                    else:
                        failed.append({'type': 'video', 'meeting': title, 'url': video_url})

        # Audio
        if download_audio:
            audio_url = meeting.get('audio_link')
            if audio_url and pd.notna(audio_url) and str(audio_url).strip():
                already, _ = self.is_file_downloaded(audio_url, title, date, 'audio')
                if not already:
                    filename = self.get_filename_from_url(audio_url, title, date, 'audio')
                    local_path = self.download_dir / 'audio' / filename
                    logger.info(f"Downloading audio: {title} ({date})")
                    size = self.download_file(audio_url, local_path)
                    if size > 0:
                        self.add_to_archive(audio_url, title, date, 'audio', local_path, size)
                        downloaded.append({'type': 'audio', 'meeting': title, 'url': audio_url})
                    else:
                        failed.append({'type': 'audio', 'meeting': title, 'url': audio_url})

        # Documents
        if download_docs:
            doc_links = []
            for field_name in ['agenda_pdf', 'agenda_html', 'minutes_pdf', 'minutes_html', 'transcript_url']:
                link = meeting.get(field_name)
                if isinstance(link, str) and link:
                    file_type = 'transcript' if field_name == 'transcript_url' else 'document'
                    doc_links.append((link, file_type))

            for url, file_type in doc_links:
                already, _ = self.is_file_downloaded(url, title, date, file_type)
                if not already:
                    filename = self.get_filename_from_url(url, title, date, file_type)
                    local_path = self.download_dir / 'documents' / filename
                    logger.info(f"Downloading {file_type}: {title} ({date})")
                    size = self.download_file(url, local_path)
                    if size > 0:
                        self.add_to_archive(url, title, date, file_type, local_path, size)
                        downloaded.append({'type': file_type, 'meeting': title, 'url': url})
                    else:
                        failed.append({'type': file_type, 'meeting': title, 'url': url})

        return downloaded, failed

    def filter_meetings(self, df, meeting_types=None, date_range=None, limit=None):
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

    def download_all(self, meeting_types=None, date_range=None, limit=None,
                     download_videos=True, download_audio=True, download_docs=True, retry_failed=False):
        """Download all requested files based on filters."""
        if retry_failed:
            meetings_to_download = self.load_failed_downloads()
            if not meetings_to_download:
                logger.info("No failed downloads to retry.")
                return
            logger.info(f"Retrying {len(meetings_to_download)} failed downloads.")
            # Clear the failed downloads log before retrying
            self.failed_downloads = []
            self.save_failed_downloads()
        else:
            df = self.load_csv_data()
            if df is None:
                return
            filtered_df = self.filter_meetings(df, meeting_types, date_range, limit)
            if len(filtered_df) == 0:
                logger.warning("No meetings match the specified criteria")
                return
            meetings_to_download = filtered_df.to_dict('records')

        logger.info(f"Starting download of files for {len(meetings_to_download)} meetings...")
        logger.info(f"Archive contains {len(self.archive_data['downloads'])} previously downloaded files")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._download_worker, meeting, download_videos, download_audio, download_docs): meeting for meeting in meetings_to_download}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading Meetings"):
                meeting = futures[future]
                try:
                    downloaded, failed = future.result()
                    self.downloaded_files.extend(downloaded)
                    self.failed_downloads.extend(failed)
                except Exception as e:
                    logger.error(f"Error processing meeting {meeting.get('title', '')}: {e}")
                    self.failed_downloads.append({'type': 'unknown', 'meeting': meeting.get('title', ''), 'url': '', 'error': str(e)})

        self.save_archive()
        self.save_failed_downloads()
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
    parser.add_argument('--max-workers', type=int, default=5, help='Number of parallel download workers (default: 5)')
    parser.add_argument('--retry-failed', action='store_true', help='Retry downloads that failed in the previous run')
    parser.add_argument('--no-videos', action='store_true', help='Skip video downloads')
    parser.add_argument('--no-audio', action='store_true', help='Skip audio downloads')
    parser.add_argument('--no-docs', action='store_true', help='Skip document and transcript downloads')
    parser.add_argument('--show-archive', action='store_true', help='Show archive statistics and exit')
    args = parser.parse_args()
    try:
        downloader = EnhancedFortCollinsVideoDownloader(args.csv, args.output, args.max_workers)
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
            download_docs=not args.no_docs,
            retry_failed=args.retry_failed
        )
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Download failed: {e}")


if __name__ == '__main__':
    main()
