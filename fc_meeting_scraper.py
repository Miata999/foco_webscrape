#!/usr/bin/env python3
"""
Extended Fort Collins Meeting Video Scraper

This script is based on the original ``fc_meeting_scraper.py`` from the
``Miata999/foco_webscrape`` repository.  The enhancements below make it
possible to scrape additional meeting types beyond the City Council and
Urban Renewal Authority meetings that the original version focused on.

## Key Enhancements

* **Broader meeting coverage** – In addition to City Council and Urban
  Renewal Authority meetings, this scraper now includes the following
  bodies:

  - **Historic Preservation Commission**
  - **Planning & Zoning Commission**

  As a result, you can gather metadata, video download links and (where
  available) English transcripts for Historic Preservation and
  Planning & Zoning meetings alongside the City Council and URA sessions.

* **Transcript support** – When a direct MP4 download link is found on
  the Cablecast platform, the scraper checks for a companion
  ``transcript.en.txt`` file in the same directory.  If the
  transcript exists, its URL is saved so that downstream tools can
  download the machine‑generated caption file.

* **Customizable meeting classification** – The categorisation logic has
  been extended so that Historic Preservation and
  Planning & Zoning meetings are assigned intuitive ``meeting_type``
  labels (e.g. ``Planning & Zoning Commission Regular Meeting``) rather
  than being lumped into generic City Council categories.  This makes
  downstream filtering via the downloader more accurate.

These changes are intended as a drop‑in replacement for the original
``fc_meeting_scraper.py``.  All of the original functionality is
preserved; you can continue to run this script exactly as before, but
you will see more meeting types appear in your CSV along with a new
``transcript_url`` column.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin, urlparse
import time
import logging
from datetime import datetime
import os
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FortCollinsVideoScraper:
    """Scrape meeting metadata, video links and transcripts for Fort Collins city bodies."""

    def __init__(self) -> None:
        # Multiple sources for Fort Collins videos
        self.sources = {
            'municode_meetings': 'https://fortcollins-co.municodemeetings.com',
            'cablecast_archive': 'https://www.fcgov.com/fctv/video-archive',
            'cablecast_api': 'https://reflect-vod-fcgov.cablecast.tv'
        }

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        self.meetings_data = []
        self.processed_urls = set()  # Track processed videos to avoid duplicates

    def fetch_page(self, url: str, max_retries: int = 3):
        """Fetch a page with retry logic."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None

    def is_fort_collins_meeting(self, title: str) -> bool:
        """
        Determine whether a given meeting title corresponds to a Fort Collins
        governmental body of interest.  The original scraper only allowed
        City Council and URA content; this method extends the whitelist to
        include Historic Preservation and Planning & Zoning commissions.  It
        still excludes Larimer County and other non‑Fort Collins entities.
        """
        title_lower = title.lower()

        # Exclude Larimer County and other entities
        exclude_terms = [
            'larimer county', 'larimer co', 'bocc', 'board of county commissioners',
            'county administrative', 'county planning', 'county land use',
            'board of social services', 'environmental stewardship'
        ]
        if any(term in title_lower for term in exclude_terms):
            return False

        # Include Fort Collins governmental bodies of interest
        include_terms = [
            'fort collins city council', 'city council', 'fc city council',
            'fort collins council',
            'urban renewal authority', 'urban renewal', 'fort collins urban renewal',
            # New bodies added
            'historic preservation commission',
            'historic preservation',
            'planning & zoning commission',
            'planning and zoning commission'
        ]
        return any(term in title_lower for term in include_terms)

    def scrape_municode_meetings(self):
        """Scrape meeting information from the Municode meetings portal."""
        logger.info("Scraping Municode meetings portal...")
        response = self.fetch_page(self.sources['municode_meetings'])
        if not response:
            return
        soup = BeautifulSoup(response.content, 'html.parser')
        meeting_rows = soup.find_all('tr')
        for row in meeting_rows:
            try:
                meeting_data = self.extract_municode_meeting_data(row)
                if meeting_data and self.is_fort_collins_meeting(meeting_data['title']):
                    unique_key = f"{meeting_data['date']}_{meeting_data['title']}"
                    if unique_key not in self.processed_urls:
                        self.meetings_data.append(meeting_data)
                        self.processed_urls.add(unique_key)
                        logger.info(f"Added (municode): {meeting_data['title']} - {meeting_data['date']}")
            except Exception as e:
                logger.warning(f"Error parsing municode row: {e}")
                continue

    def extract_municode_meeting_data(self, row) -> dict | None:
        """Extract meeting data from Municode table row."""
        cells = row.find_all('td')
        if len(cells) < 2:
            return None
        meeting_data: dict[str, str] = {
            'date': '',
            'time': '',
            'title': '',
            'meeting_type': '',
            'source': 'municode',
            'agenda_pdf': '',
            'agenda_html': '',
            'minutes_pdf': '',
            'minutes_html': '',
            'audio_link': '',
            'video_link': '',
            'mp4_download': '',
            'detail_page': '',
            'transcript_url': ''
        }
        # Extract date and time
        date_time_cell = cells[0] if cells else None
        if date_time_cell:
            date_time_text = date_time_cell.get_text(strip=True)
            if ' - ' in date_time_text:
                date_part, time_part = date_time_text.split(' - ', 1)
                meeting_data['date'] = date_part.strip()
                meeting_data['time'] = time_part.strip()
        # Extract title and categorize
        title_cell = cells[1] if len(cells) > 1 else None
        if title_cell:
            meeting_data['title'] = title_cell.get_text(strip=True)
            meeting_data['meeting_type'] = self.categorize_meeting_type(meeting_data['title'])
        # Extract links for video/audio/documents
        for cell in cells:
            links = cell.find_all('a')
            images = cell.find_all('img')
            for link in links:
                href = link.get('href', '')
                if href and 'view details' in link.get_text(strip=True).lower():
                    meeting_data['detail_page'] = urljoin(self.sources['municode_meetings'], href)
            for img in images:
                src = img.get('src', '')
                parent_link = img.find_parent('a')
                if parent_link:
                    href = parent_link.get('href', '')
                    if href:
                        full_url = urljoin(self.sources['municode_meetings'], href)
                        if 'video' in src:
                            meeting_data['video_link'] = full_url
                        elif 'pdf' in src:
                            if not meeting_data['agenda_pdf']:
                                meeting_data['agenda_pdf'] = full_url
                            else:
                                meeting_data['minutes_pdf'] = full_url
        return meeting_data if meeting_data['title'] else None

    def scrape_cablecast_videos(self):
        """Scrape Fort Collins videos from the Cablecast platform using search."""
        logger.info("Scraping Cablecast video platform via search...")
        search_terms = [
            # City Council
            "Fort Collins City Council Regular Meeting",
            "Fort Collins City Council Work Session",
            "Fort Collins City Council Special Meeting",
            "Fort Collins City Council Meeting",
            # Urban Renewal Authority
            "Urban Renewal Authority",
            "Fort Collins Urban Renewal Authority",
            "Urban Renewal Authority Board",
            # Historic Preservation Commission
            "Historic Preservation Commission Meeting",
            "Historic Preservation Commission Regular Meeting",
            "Historic Preservation Commission",
            # Planning & Zoning Commission
            "Planning & Zoning Commission Meeting",
            "Planning & Zoning Commission Regular Meeting",
            "Planning and Zoning Commission"
        ]
        for search_term in search_terms:
            self.search_cablecast_videos(search_term)
            time.sleep(2)  # polite delay

    def search_cablecast_videos(self, search_term: str) -> None:
        """Execute multiple search queries on the Cablecast platform for a term."""
        search_urls = [
            f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/search?q={search_term.replace(' ', '+')}&site=1",
            f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/search?q={search_term.replace(' ', '+')}&site=1"
        ]
        for search_url in search_urls:
            try:
                response = self.fetch_page(search_url)
                if response:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    self.extract_cablecast_videos_from_page(soup)
            except Exception as e:
                logger.warning(f"Error searching Cablecast: {e}")

    def extract_cablecast_videos_from_page(self, soup: BeautifulSoup) -> None:
        """Extract video listings from a Cablecast search results page."""
        try:
            video_links = soup.find_all('a', href=re.compile(r'/show/\d+'))
            for link in video_links:
                href = link.get('href', '')
                if not href:
                    continue
                full_url = urljoin('https://reflect-vod-fcgov.cablecast.tv', href)
                # Determine title text
                title_text = link.get_text(strip=True)
                parent = link.parent
                if parent:
                    parent_text = parent.get_text(strip=True)
                    if len(parent_text) > len(title_text):
                        title_text = parent_text
                if self.is_fort_collins_meeting(title_text):
                    video_id_match = re.search(r'/show/(\d+)', href)
                    if video_id_match:
                        video_id = int(video_id_match.group(1))
                        video_response = self.fetch_page(full_url)
                        if video_response:
                            video_soup = BeautifulSoup(video_response.content, 'html.parser')
                            meeting_data = self.extract_cablecast_video_data(video_soup, full_url, video_id)
                            if meeting_data:
                                unique_key = f"{meeting_data['date']}_{meeting_data['title']}"
                                if unique_key not in self.processed_urls:
                                    self.meetings_data.append(meeting_data)
                                    self.processed_urls.add(unique_key)
                                    logger.info(f"Found via search: {meeting_data['title']}")
                        time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error extracting videos from search page: {e}")

    def scrape_cablecast_archive_systematic(self):
        """Systematically check ranges of Cablecast video IDs for Fort Collins content."""
        logger.info("Systematically checking Cablecast video archive...")
        ranges_to_check = [
            (2800, 2900),
            (2600, 2800),
            (2200, 2600),
            (1800, 2200),
            (1400, 1800),
            (1000, 1400),
            (500, 1000),
        ]
        for start_id, end_id in ranges_to_check:
            logger.info(f"Checking video IDs {start_id}-{end_id}")
            self.check_cablecast_id_range(start_id, end_id)

    def check_cablecast_id_range(self, start_id: int, end_id: int) -> None:
        """Iterate through a range of show IDs and collect Fort Collins meetings."""
        fort_collins_count = 0
        for video_id in range(start_id, end_id):
            try:
                urls_to_try = [
                    f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/show/{video_id}?site=1",
                    f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/show/{video_id}?channel=1"
                ]
                for url in urls_to_try:
                    response = self.fetch_page(url)
                    if response and response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        title_elem = soup.find('h1') or soup.find('title') or soup.find('h2')
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            if self.is_fort_collins_meeting(title):
                                meeting_data = self.extract_cablecast_video_data(soup, url, video_id)
                                if meeting_data:
                                    unique_key = f"{meeting_data['date']}_{meeting_data['title']}"
                                    if unique_key not in self.processed_urls:
                                        self.meetings_data.append(meeting_data)
                                        self.processed_urls.add(unique_key)
                                        fort_collins_count += 1
                                        logger.info(f"Found Fort Collins video: {title}")
                                        break
                        break
                if video_id % 50 == 0:
                    logger.info(f"Checked up to ID {video_id}, found {fort_collins_count} Fort Collins videos so far")
                    time.sleep(1)
            except Exception as e:
                if "404" not in str(e):
                    logger.debug(f"Error checking video {video_id}: {e}")

    def extract_cablecast_video_data(self, soup: BeautifulSoup, page_url: str, video_id: int) -> dict | None:
        """Extract meeting data, MP4 download link and transcript URL from a Cablecast video page."""
        try:
            meeting_data: dict[str, str] = {
                'date': '',
                'time': '',
                'title': '',
                'meeting_type': '',
                'source': 'cablecast',
                'video_id': video_id,
                'agenda_pdf': '',
                'agenda_html': '',
                'minutes_pdf': '',
                'minutes_html': '',
                'audio_link': '',
                'video_link': page_url,
                'mp4_download': '',
                'detail_page': page_url,
                'transcript_url': ''
            }
            # Extract title and categorize
            title_elem = soup.find('h1') or soup.find('title') or soup.find('h2')
            if title_elem:
                title = title_elem.get_text(strip=True)
                meeting_data['title'] = title
                meeting_data['meeting_type'] = self.categorize_meeting_type(title)
                # Parse date and time from title when present
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', title)
                if date_match:
                    meeting_data['date'] = date_match.group(1)
                time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', title, re.IGNORECASE)
                if time_match:
                    meeting_data['time'] = time_match.group(1)
            # Find MP4 download link
            download_links = soup.find_all('a', href=re.compile(r'\.mp4$'))
            for link in download_links:
                href = link.get('href')
                if href:
                    if href.startswith('http'):
                        meeting_data['mp4_download'] = href
                    else:
                        meeting_data['mp4_download'] = urljoin('https://reflect-vod-fcgov.cablecast.tv/', href)
                    break
            # Fallback: parse page text for .mp4 URL
            if not meeting_data['mp4_download']:
                page_text = soup.get_text()
                mp4_matches = re.findall(r'https://[^\s]*\.mp4', page_text)
                if mp4_matches:
                    meeting_data['mp4_download'] = mp4_matches[0]
            # Attempt to determine transcript URL
            if meeting_data['mp4_download']:
                try:
                    base_path = meeting_data['mp4_download'].rsplit('/', 1)[0]
                    candidate_transcript = f"{base_path}/transcript.en.txt"
                    head_resp = self.session.head(candidate_transcript, timeout=10)
                    if head_resp.status_code == 200:
                        meeting_data['transcript_url'] = candidate_transcript
                except Exception:
                    # If a HEAD request fails, we silently ignore transcript
                    pass
            return meeting_data if meeting_data['title'] else None
        except Exception as e:
            logger.error(f"Error extracting Cablecast data: {e}")
            return None

    def categorize_meeting_type(self, title: str) -> str:
        """Assign a human‑readable meeting type based on keywords in the title."""
        title_lower = title.lower()
        # Historic Preservation
        if 'historic preservation' in title_lower:
            if 'regular' in title_lower:
                return 'Historic Preservation Commission Regular Meeting'
            return 'Historic Preservation Commission Meeting'
        # Planning & Zoning
        if 'planning & zoning' in title_lower or 'planning and zoning' in title_lower:
            if 'regular' in title_lower:
                return 'Planning & Zoning Commission Regular Meeting'
            return 'Planning & Zoning Commission Meeting'
        # Urban Renewal
        if 'urban renewal' in title_lower:
            if 'workshop' in title_lower:
                return 'Urban Renewal Authority Workshop'
            return 'Urban Renewal Authority Board Meeting'
        # City Council variants
        if 'regular meeting' in title_lower or 'regular' in title_lower:
            return 'City Council Regular Meeting'
        if 'work session' in title_lower:
            return 'City Council Work Session'
        if 'special meeting' in title_lower or 'special' in title_lower:
            return 'City Council Special Meeting'
        if 'adjourned' in title_lower:
            return 'City Council Adjourned Meeting'
        return 'City Council Meeting'

    def enhance_with_additional_data(self):
        """Enhance meeting data by visiting detail pages for additional links."""
        logger.info("Enhancing data with additional information from detail pages...")
        for meeting in self.meetings_data:
            if meeting.get('detail_page') and not meeting.get('mp4_download'):
                try:
                    response = self.fetch_page(meeting['detail_page'])
                    if response:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        video_links = soup.find_all('a', href=re.compile(r'(video|stream|mp4|watch)', re.I))
                        for link in video_links:
                            href = link.get('href', '')
                            if 'mp4' in href and not meeting['mp4_download']:
                                meeting['mp4_download'] = urljoin(meeting['detail_page'], href)
                                break
                            elif ('cablecast' in href or 'stream' in href) and not meeting['video_link']:
                                meeting['video_link'] = urljoin(meeting['detail_page'], href)
                        # If we found an MP4 now, attempt transcript again
                        if meeting['mp4_download'] and not meeting['transcript_url']:
                            try:
                                base_path = meeting['mp4_download'].rsplit('/', 1)[0]
                                candidate_transcript = f"{base_path}/transcript.en.txt"
                                head_resp = self.session.head(candidate_transcript, timeout=10)
                                if head_resp.status_code == 200:
                                    meeting['transcript_url'] = candidate_transcript
                            except Exception:
                                pass
                        time.sleep(1)
                except Exception as e:
                    logger.warning(f"Error enhancing meeting data: {e}")

    def save_to_csv(self, filename: str = 'fort_collins_all_meetings.csv') -> None:
        """Save collected meeting data to a CSV file."""
        if not self.meetings_data:
            logger.warning("No meeting data to save")
            return
        df = pd.DataFrame(self.meetings_data)
        # Sort by date descending when available
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values('date_parsed', ascending=False, na_position='last')
        df = df.drop('date_parsed', axis=1)
        # Deduplicate by title and date
        df = df.drop_duplicates(subset=['title', 'date'], keep='first')
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} meetings to {filename}")
        self.print_summary(df, filename)

    def print_summary(self, df: pd.DataFrame, filename: str) -> None:
        """Print a summary of scraped data to the console."""
        print("\n=== COMPREHENSIVE SCRAPING SUMMARY ===")
        print(f"Total meetings found: {len(df)}")
        print("\nMeeting types found:")
        type_counts = df['meeting_type'].value_counts()
        for mtype, count in type_counts.items():
            print(f"  {mtype}: {count}")
        print("\nSources:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            print(f"  {source}: {count}")
        print("\nVideo availability:")
        print(f"  Meetings with MP4 downloads: {df['mp4_download'].notna().sum()}")
        print(f"  Meetings with video links: {df['video_link'].notna().sum()}")
        print(f"  Meetings with transcripts: {df['transcript_url'].notna().sum()}")
        print(f"\nCSV saved as: {filename}\n")

    def run_comprehensive_scraper(self) -> None:
        """Run the end‑to‑end scraping process across all sources."""
        logger.info("Starting comprehensive Fort Collins meeting scraper...")
        try:
            logger.info("=== Phase 1: Municode Meetings Portal ===")
            self.scrape_municode_meetings()
            logger.info("=== Phase 2: Cablecast Search ===")
            self.scrape_cablecast_videos()
            logger.info("=== Phase 3: Systematic Cablecast Archive Check ===")
            self.scrape_cablecast_archive_systematic()
            if not self.meetings_data:
                logger.error("No meeting data found from any source")
                return
            logger.info("=== Phase 4: Enhanced Data Collection ===")
            self.enhance_with_additional_data()
            logger.info("=== Phase 5: Saving Results ===")
            self.save_to_csv()
            logger.info("Scraping completed successfully!")
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise


def main() -> None:
    """Entry point when running as a script."""
    try:
        scraper = FortCollinsVideoScraper()
        scraper.run_comprehensive_scraper()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")


if __name__ == "__main__":
    main()