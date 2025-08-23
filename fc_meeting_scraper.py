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
from typing import Union, Dict, Optional, List

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

    def _extract_media_urls_from_html(self, soup: BeautifulSoup, base_url: str) -> Dict[str, List[str]]:
        """Extract media URLs from common HTML elements and script text.

        Returns a dict with keys: mp4, mpeg, m3u8 containing URL lists.
        """
        media: Dict[str, List[str]] = {'mp4': [], 'mpeg': [], 'm3u8': []}

        def add_url(kind: str, url_val: str) -> None:
            if not url_val:
                return
            if not url_val.startswith('http'):
                url_val = urljoin(base_url, url_val)
            if url_val not in media[kind]:
                media[kind].append(url_val)

        # Elements with src/href
        for tag_name, attr in [('a', 'href'), ('source', 'src'), ('video', 'src'), ('link', 'href')]:
            for tag in soup.find_all(tag_name):
                val = tag.get(attr, '')
                if not val:
                    continue
                low = val.lower()
                if '.mp4' in low:
                    add_url('mp4', val)
                if '.mpeg' in low:
                    add_url('mpeg', val)
                if '.m3u8' in low:
                    add_url('m3u8', val)

        # Meta video
        for meta in soup.find_all('meta'):
            prop = (meta.get('property') or meta.get('name') or '').lower()
            if 'video' in prop:
                content = meta.get('content', '')
                if '.mp4' in (content or '').lower():
                    add_url('mp4', content)

        # Script text scanning
        combined_script = '\n'.join([s.get_text(' ', strip=False) for s in soup.find_all('script')])
        if combined_script:
            for pattern, kind in [
                (r'https?://[^"\'\s>]+\.mp4[^"\'\s>]*', 'mp4'),
                (r'https?://[^"\'\s>]+\.mpeg[^"\'\s>]*', 'mpeg'),
                (r'https?://[^"\'\s>]+\.m3u8[^"\'\s>]*', 'm3u8'),
            ]:
                try:
                    for match in re.findall(pattern, combined_script, flags=re.IGNORECASE):
                        add_url(kind, match)
                except re.error:
                    continue

        return media

    def _follow_embeds_and_players(self, page_url: str, soup: BeautifulSoup, video_id: Optional[int]) -> Dict[str, List[str]]:
        """Follow iframes/player links likely to contain direct media URLs and aggregate results."""
        aggregated: Dict[str, List[str]] = {'mp4': [], 'mpeg': [], 'm3u8': []}

        def merge(found: Dict[str, List[str]]):
            for k in aggregated.keys():
                for u in found.get(k, []):
                    if u not in aggregated[k]:
                        aggregated[k].append(u)

        # Discover iframe/player links from current soup
        candidate_links: List[str] = []
        for iframe in soup.find_all(['iframe']):
            src = iframe.get('src', '')
            if src:
                candidate_links.append(urljoin(page_url, src))
        # Anchor links that look like players or embeds
        for a in soup.find_all('a'):
            href = a.get('href', '')
            if href and any(token in href.lower() for token in ['embed', 'iframe', 'player']):
                candidate_links.append(urljoin(page_url, href))

        # Try common Cablecast embed patterns by ID if known
        if video_id is not None:
            embed_candidates = [
                f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/resource/embed/iframe?show={video_id}&site=1",
                f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/embed?show={video_id}&site=1",
                f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/resource/embed/iframe?show={video_id}&site=1",
            ]
            candidate_links.extend(embed_candidates)

        # Fetch each candidate and extract media
        for url in list(dict.fromkeys(candidate_links)):
            try:
                resp = self.fetch_page(url)
                if not resp:
                    continue
                child_soup = BeautifulSoup(resp.content, 'html.parser')
                media = self._extract_media_urls_from_html(child_soup, url)
                merge(media)
            except Exception as e:
                logger.debug(f"Error following embed {url}: {e}")

        return aggregated

    def _pick_best_media_for_id(self, candidates: List[str], video_id: Optional[int]) -> Optional[str]:
        """Pick the most likely direct media URL for a specific show ID."""
        if not candidates:
            return None
        # Deduplicate preserving order
        unique = []
        seen = set()
        for u in candidates:
            if u not in seen:
                unique.append(u)
                seen.add(u)

        def score(url: str) -> int:
            s = 0
            try:
                parsed = urlparse(url)
                if parsed.netloc.endswith('cablecast.tv'):
                    s += 1
                if video_id is not None:
                    vid_str = str(int(video_id))
                    path = parsed.path or ''
                    query = parsed.query or ''
                    if f"/{vid_str}-" in path or f"/{vid_str}_" in path or f"/{vid_str}/" in path:
                        s += 6
                    if f"show={vid_str}" in query:
                        s += 3
                    # Common store pattern: /store-X/<id>-Title/vod.mp4
                    if '/store-' in path and f"/{vid_str}-" in path:
                        s += 4
            except Exception:
                pass
            return s

        best = max(unique, key=score)
        # If all scores are zero, fall back to the first candidate
        if score(best) == 0:
            return unique[0]
        return best

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

    def extract_municode_meeting_data(self, row) -> Optional[Dict[str, str]]:
        """Extract meeting data from Municode table row."""
        cells = row.find_all('td')
        if len(cells) < 2:
            return None
        meeting_data: Dict[str, str] = {
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
        """Scrape Fort Collins videos from the Cablecast platform using galleries and search."""
        logger.info("Scraping Cablecast video platform via galleries...")
        # First scrape the organized galleries - this is the primary method
        self.scrape_cablecast_galleries()
        
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

    def scrape_cablecast_galleries(self):
        """Scrape videos from the organized Cablecast galleries."""
        galleries = [
            {'id': 3, 'name': 'City Council Meetings', 'total_expected': 112},
            {'id': 5, 'name': 'Urban Renewal Authority Board Meetings', 'total_expected': 4},
            {'id': 6, 'name': 'Planning & Zoning Commission Meetings', 'total_expected': 35},
            {'id': 4, 'name': 'Historic Preservation Commission Meetings', 'total_expected': 26}
        ]
        
        for gallery in galleries:
            logger.info(f"Scraping gallery: {gallery['name']} (ID {gallery['id']})")
            self.scrape_single_gallery(gallery['id'], gallery['name'])
            time.sleep(2)

    def scrape_single_gallery(self, gallery_id: int, gallery_name: str):
        """Scrape all pages of a single gallery."""
        page = 1
        videos_found = 0
        
        while True:
            # Construct gallery URL with pagination
            if page == 1:
                url = f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/gallery/{gallery_id}?site=1"
            else:
                offset = (page - 1) * 50
                url = f"https://reflect-vod-fcgov.cablecast.tv/internetchannel/gallery/{gallery_id}?page={page}&fullText=null&page_size=50&offset={offset}&site=1"
            
            logger.info(f"Fetching {gallery_name} page {page}: {url}")
            response = self.fetch_page(url)
            
            if not response:
                logger.warning(f"Failed to fetch {gallery_name} page {page}")
                break
                
            soup = BeautifulSoup(response.content, 'html.parser')
            page_videos = self.extract_gallery_videos(soup, gallery_id)
            
            if not page_videos:
                logger.info(f"No videos found on {gallery_name} page {page}, stopping pagination")
                break
                
            videos_found += len(page_videos)
            logger.info(f"Found {len(page_videos)} videos on {gallery_name} page {page}")
            
            # Check if there are more pages by looking for pagination elements
            has_next_page = self.has_next_page(soup)
            if not has_next_page:
                break
                
            page += 1
            time.sleep(1)  # Be polite
            
        logger.info(f"Gallery {gallery_name} complete: {videos_found} videos found")

    def extract_gallery_videos(self, soup: BeautifulSoup, gallery_id: int) -> List[Dict]:
        """Extract video data from a gallery page."""
        videos_found = []
        
        # Look for video links in the gallery page
        video_links = soup.find_all('a', href=re.compile(r'/internetchannel/show/\d+'))
        if not video_links:
            # Alternative: look for any show links
            video_links = soup.find_all('a', href=re.compile(r'/show/\d+'))
            
        for link in video_links:
            href = link.get('href', '')
            if not href:
                continue
                
            # Extract video ID
            video_id_match = re.search(r'/show/(\d+)', href)
            if not video_id_match:
                continue
                
            video_id = int(video_id_match.group(1))
            
            # Get title from link text or nearby elements
            title_text = link.get_text(strip=True)
            if not title_text:
                # Try to get title from parent or sibling elements
                parent = link.parent
                if parent:
                    title_text = parent.get_text(strip=True)
                    
            if not title_text or not self.is_fort_collins_meeting(title_text):
                continue
                
            # Build full URL
            if href.startswith('/'):
                full_url = f"https://reflect-vod-fcgov.cablecast.tv{href}"
            else:
                full_url = href
                
            # Add site parameter if not present
            if '?site=1' not in full_url and '&site=1' not in full_url:
                separator = '&' if '?' in full_url else '?'
                full_url += f"{separator}site=1"
                
            # Fetch the individual video page to get full metadata
            unique_key = f"gallery_{video_id}_{title_text}"
            if unique_key not in self.processed_urls:
                video_response = self.fetch_page(full_url)
                if video_response:
                    video_soup = BeautifulSoup(video_response.content, 'html.parser')
                    meeting_data = self.extract_cablecast_video_data(video_soup, full_url, video_id)
                    if meeting_data:
                        self.meetings_data.append(meeting_data)
                        self.processed_urls.add(unique_key)
                        videos_found.append(meeting_data)
                        logger.info(f"Added from gallery {gallery_id}: {meeting_data['title']}")
                time.sleep(0.5)  # Be polite
                
        return videos_found

    def has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page in the gallery pagination."""
        # Look for pagination elements
        next_links = soup.find_all('a', string=re.compile(r'Next|>', re.I))
        if next_links:
            return True
            
        # Look for page numbers higher than current
        page_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        if len(page_links) > 1:  # More than just current page
            return True
            
        # Look for specific pagination patterns
        pagination_div = soup.find('div', class_=re.compile(r'pag', re.I))
        if pagination_div:
            # If pagination div exists and has multiple links, likely has more pages
            links_in_pagination = pagination_div.find_all('a')
            return len(links_in_pagination) > 1
            
        return False

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

    def extract_cablecast_video_data(self, soup: BeautifulSoup, page_url: str, video_id: int) -> Optional[Dict[str, str]]:
        """Extract meeting data, MP4 download link and transcript URL from a Cablecast video page."""
        try:
            meeting_data: Dict[str, str] = {
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
            # Find direct media links aggressively
            # 1) Straightforward <a href="*.mp4">
            download_links = soup.find_all('a', href=re.compile(r'\.mp4($|\?)', re.I))
            for link in download_links:
                href = link.get('href')
                if href:
                    if href.startswith('http'):
                        meeting_data['mp4_download'] = href
                    else:
                        meeting_data['mp4_download'] = urljoin(page_url, href)
                    break

            # 2) Scan common elements and scripts
            if not meeting_data['mp4_download']:
                media = self._extract_media_urls_from_html(soup, page_url)
                if media.get('mp4'):
                    meeting_data['mp4_download'] = self._pick_best_media_for_id(media['mp4'], video_id)
                elif media.get('mpeg'):
                    meeting_data['mp4_download'] = self._pick_best_media_for_id(media['mpeg'], video_id)
                elif media.get('m3u8'):
                    # Best-effort: attempt to derive an MP4 from HLS URL
                    derived = self._pick_best_media_for_id(media['m3u8'], video_id) or media['m3u8'][0]
                    candidate = derived.split('?', 1)[0].rsplit('.', 1)[0] + '.mp4'
                    try:
                        head = self.session.head(candidate, timeout=10, allow_redirects=True)
                        if head.status_code == 200 and int(head.headers.get('content-length', '0')) > 0:
                            meeting_data['mp4_download'] = candidate
                    except Exception:
                        pass

            # 3) Follow iframes/player embeds and rescan
            if not meeting_data['mp4_download']:
                embed_media = self._follow_embeds_and_players(page_url, soup, video_id)
                if embed_media.get('mp4'):
                    meeting_data['mp4_download'] = embed_media['mp4'][0]
                elif embed_media.get('mpeg'):
                    meeting_data['mp4_download'] = embed_media['mpeg'][0]

            # 4) Fallback: parse entire page text for .mp4 URL
            if not meeting_data['mp4_download']:
                page_text = soup.get_text(" ")
                mp4_matches = re.findall(r'https?://[^\s"\'>]+\.mp4[^\s"\'>]*', page_text, flags=re.IGNORECASE)
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
                        # Try anchors first
                        video_links = soup.find_all('a', href=re.compile(r'(video|stream|mp4|watch)', re.I))
                        for link in video_links:
                            href = link.get('href', '')
                            if 'mp4' in href.lower() and not meeting['mp4_download']:
                                meeting['mp4_download'] = urljoin(meeting['detail_page'], href)
                                break
                            elif ('cablecast' in href.lower() or 'stream' in href.lower()) and not meeting['video_link']:
                                meeting['video_link'] = urljoin(meeting['detail_page'], href)

                        # If not found, scan DOM and scripts for media URLs
                        if not meeting['mp4_download']:
                            media = self._extract_media_urls_from_html(soup, meeting['detail_page'])
                            if media.get('mp4'):
                                meeting['mp4_download'] = self._pick_best_media_for_id(media['mp4'], meeting.get('video_id'))
                            elif media.get('mpeg'):
                                meeting['mp4_download'] = self._pick_best_media_for_id(media['mpeg'], meeting.get('video_id'))
                            elif media.get('m3u8'):
                                derived = self._pick_best_media_for_id(media['m3u8'], meeting.get('video_id')) or media['m3u8'][0]
                                candidate = derived.split('?', 1)[0].rsplit('.', 1)[0] + '.mp4'
                                try:
                                    head = self.session.head(candidate, timeout=10, allow_redirects=True)
                                    if head.status_code == 200 and int(head.headers.get('content-length', '0')) > 0:
                                        meeting['mp4_download'] = candidate
                                except Exception:
                                    pass

                        # If still not found, follow embeds from this page
                        if not meeting['mp4_download']:
                            # Try to deduce video_id from stored data or URL
                            video_id = None
                            try:
                                if meeting.get('video_id'):
                                    video_id = int(meeting['video_id'])
                                else:
                                    m = re.search(r'/show/(\d+)', meeting.get('video_link') or '')
                                    if m:
                                        video_id = int(m.group(1))
                            except Exception:
                                video_id = None
                            embed_media = self._follow_embeds_and_players(meeting['detail_page'], soup, video_id)
                            if embed_media.get('mp4'):
                                meeting['mp4_download'] = embed_media['mp4'][0]
                            elif embed_media.get('mpeg'):
                                meeting['mp4_download'] = embed_media['mpeg'][0]
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

    def run_comprehensive_scraper(self, quick: bool = False) -> None:
        """Run the end‑to‑end scraping process across all sources.

        If quick=True, skips the slow systematic ID range scan and saves after earlier phases.
        """
        logger.info("Starting comprehensive Fort Collins meeting scraper...")
        try:
            # Phase 1: Municode
            logger.info("=== Phase\u00a01: Municode Meetings Portal ===")
            self.scrape_municode_meetings()

            # Phase 2: Cablecast (galleries + search)
            logger.info("=== Phase\u00a02: Cablecast Search ===")
            self.scrape_cablecast_videos()

            if not self.meetings_data:
                logger.error("No meeting data found from any source after Phase 2")
                return

            # Enhance and save a partial checkpoint after early phases
            logger.info("=== Phase\u00a03: Enhanced Data Collection (early) ===")
            self.enhance_with_additional_data()
            logger.info("=== Saving partial results (post Phase 2/3) ===")
            self.save_to_csv()

            # Optional Phase 4: Systematic scan (slow)
            if not quick:
                logger.info("=== Phase\u00a04: Systematic Cablecast Archive Check ===")
                self.scrape_cablecast_archive_systematic()
                if self.meetings_data:
                    logger.info("=== Phase\u00a05: Enhanced Data Collection (final) ===")
                    self.enhance_with_additional_data()
                    logger.info("=== Phase\u00a06: Saving Results ===")
                    self.save_to_csv()
            logger.info("Scraping completed successfully!")
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise


def main() -> None:
    """Entry point when running as a script."""
    import argparse
    parser = argparse.ArgumentParser(description='Fort Collins meeting scraper')
    parser.add_argument('--quick', action='store_true', help='Skip slow ID-range scan; save after galleries/search')
    args = parser.parse_args()
    scraper = FortCollinsVideoScraper()
    try:
        scraper.run_comprehensive_scraper(quick=args.quick)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        # Save whatever we have so far to avoid losing progress
        try:
            if scraper.meetings_data:
                logger.info("Saving partial results before exit due to interruption...")
                scraper.save_to_csv()
        except Exception as e:
            logger.warning(f"Failed to save partial results: {e}")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")


if __name__ == "__main__":
    main()