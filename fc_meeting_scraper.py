#!/usr/bin/env python3
"""
Fort Collins City Council Video Scraper - Enhanced Version
Scrapes ALL Fort Collins City Council videos from multiple sources including Cablecast platform
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
    def __init__(self):
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
        
    def fetch_page(self, url, max_retries=3):
        """Fetch a page with retry logic"""
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
    
    def is_fort_collins_city_council(self, title):
        """Check if this is a Fort Collins City Council or Urban Renewal Authority video (not Larimer County or other)"""
        title_lower = title.lower()
        
        # Exclude Larimer County and other entities
        exclude_terms = [
            'larimer county', 'larimer co', 'bocc', 'board of county commissioners',
            'county administrative', 'county planning', 'county land use',
            'board of social services', 'environmental stewardship'
        ]
        
        if any(term in title_lower for term in exclude_terms):
            return False
        
        # Include Fort Collins City Council AND Urban Renewal Authority
        include_terms = [
            'fort collins city council', 'city council', 'fc city council',
            'fort collins council', 'urban renewal authority', 'urban renewal',
            'fort collins urban renewal'
        ]
        
        return any(term in title_lower for term in include_terms)
    
    def scrape_municode_meetings(self):
        """Scrape from the Municode meetings portal"""
        logger.info("Scraping Municode meetings portal...")
        
        response = self.fetch_page(self.sources['municode_meetings'])
        if not response:
            return
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all meeting rows
        meeting_rows = soup.find_all('tr')
        
        for row in meeting_rows:
            try:
                meeting_data = self.extract_municode_meeting_data(row)
                if meeting_data and self.is_fort_collins_city_council(meeting_data['title']):
                    # Check for duplicates
                    unique_key = f"{meeting_data['date']}_{meeting_data['title']}"
                    if unique_key not in self.processed_urls:
                        self.meetings_data.append(meeting_data)
                        self.processed_urls.add(unique_key)
                        logger.info(f"Added: {meeting_data['title']} - {meeting_data['date']}")
            except Exception as e:
                logger.warning(f"Error parsing municode row: {e}")
                continue
    
    def extract_municode_meeting_data(self, row):
        """Extract meeting data from Municode table row"""
        cells = row.find_all('td')
        if len(cells) < 2:
            return None
            
        meeting_data = {
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
            'detail_page': ''
        }
        
        # Extract date and time
        date_time_cell = cells[0] if cells else None
        if date_time_cell:
            date_time_text = date_time_cell.get_text(strip=True)
            if ' - ' in date_time_text:
                date_part, time_part = date_time_text.split(' - ', 1)
                meeting_data['date'] = date_part.strip()
                meeting_data['time'] = time_part.strip()
        
        # Extract title
        title_cell = cells[1] if len(cells) > 1 else None
        if title_cell:
            meeting_data['title'] = title_cell.get_text(strip=True)
            meeting_data['meeting_type'] = self.categorize_meeting_type(meeting_data['title'])
        
        # Extract links
        for cell in cells:
            links = cell.find_all('a')
            images = cell.find_all('img')
            
            for link in links:
                href = link.get('href', '')
                if href and 'view details' in link.get_text(strip=True).lower():
                    meeting_data['detail_page'] = urljoin(self.sources['municode_meetings'], href)
            
            # Check for document icons
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
        """Scrape Fort Collins City Council videos from Cablecast platform"""
        logger.info("Scraping Cablecast video platform...")
        
        # Search for Fort Collins City Council videos on Cablecast
        search_terms = [
            "Fort Collins City Council Regular Meeting",
            "Fort Collins City Council Work Session", 
            "Fort Collins City Council Special Meeting",
            "Fort Collins City Council Meeting",
            "Urban Renewal Authority",
            "Fort Collins Urban Renewal Authority",
            "Urban Renewal Authority Board"
        ]
        
        for search_term in search_terms:
            self.search_cablecast_videos(search_term)
            time.sleep(2)  # Rate limiting
    
    def search_cablecast_videos(self, search_term):
        """Search for specific videos on Cablecast"""
        # Try different search approaches on the Cablecast platform
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
    
    def extract_cablecast_videos_from_page(self, soup):
        """Extract video data from Cablecast search results page"""
        try:
            # Look for video links in search results
            video_links = soup.find_all('a', href=re.compile(r'/show/\d+'))
            
            for link in video_links:
                href = link.get('href', '')
                if href:
                    full_url = urljoin('https://reflect-vod-fcgov.cablecast.tv', href)
                    
                    # Get the title from the link text or nearby text
                    title_text = link.get_text(strip=True)
                    
                    # Also check parent elements for title
                    parent = link.parent
                    if parent:
                        parent_text = parent.get_text(strip=True)
                        if len(parent_text) > len(title_text):
                            title_text = parent_text
                    
                    if self.is_fort_collins_city_council(title_text):
                        # Extract video ID from URL
                        video_id_match = re.search(r'/show/(\d+)', href)
                        if video_id_match:
                            video_id = int(video_id_match.group(1))
                            
                            # Fetch the video page for complete data
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
                            
                            time.sleep(0.5)  # Rate limiting
                            
        except Exception as e:
            logger.error(f"Error extracting videos from search page: {e}")
    
    def scrape_cablecast_archive_systematic(self):
        """Systematically scrape Cablecast by checking video IDs"""
        logger.info("Systematically checking Cablecast video archive...")
        
        # Based on the video URLs we've seen, try a range of show IDs
        # Recent videos seem to be in the 2000+ range, older ones in lower ranges
        
        ranges_to_check = [
            (2800, 2900),  # Very recent (2025)
            (2600, 2800),  # Recent (2024-2025) 
            (2200, 2600),  # 2023-2024
            (1800, 2200),  # 2022-2023
            (1400, 1800),  # 2021-2022
            (1000, 1400),  # 2020-2021
            (500, 1000),   # 2019-2020
        ]
        
        for start_id, end_id in ranges_to_check:
            logger.info(f"Checking video IDs {start_id}-{end_id}")
            self.check_cablecast_id_range(start_id, end_id)
    
    def check_cablecast_id_range(self, start_id, end_id):
        """Check a range of Cablecast video IDs for Fort Collins City Council content"""
        fort_collins_count = 0
        
        for video_id in range(start_id, end_id):
            try:
                # Try both URL patterns we've seen
                urls_to_try = [
                    f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/show/{video_id}?site=1",
                    f"https://reflect-vod-fcgov.cablecast.tv/CablecastPublicSite/show/{video_id}?channel=1"
                ]
                
                for url in urls_to_try:
                    response = self.fetch_page(url)
                    if response and response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Look for title
                        title_elem = soup.find('h1') or soup.find('title') or soup.find('h2')
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            
                            if self.is_fort_collins_city_council(title):
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
                        
            except Exception as e:
                if "404" not in str(e):
                    logger.debug(f"Error checking video {video_id}: {e}")
            
            # Rate limiting and progress update
            if video_id % 50 == 0:
                logger.info(f"Checked up to ID {video_id}, found {fort_collins_count} Fort Collins videos")
                time.sleep(1)
    
    def extract_cablecast_video_data(self, soup, page_url, video_id):
        """Extract video data from Cablecast video page"""
        try:
            meeting_data = {
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
                'detail_page': page_url
            }
            
            # Extract title
            title_elem = soup.find('h1') or soup.find('title') or soup.find('h2')
            if title_elem:
                title = title_elem.get_text(strip=True)
                meeting_data['title'] = title
                meeting_data['meeting_type'] = self.categorize_meeting_type(title)
                
                # Extract date from title (format like "Fort Collins City Council Regular Meeting 8/19/25")
                date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', title)
                if date_match:
                    meeting_data['date'] = date_match.group(1)
                
                # Extract time if present
                time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)', title, re.IGNORECASE)
                if time_match:
                    meeting_data['time'] = time_match.group(1)
            
            # Look for MP4 download link
            download_links = soup.find_all('a', href=re.compile(r'\.mp4$'))
            for link in download_links:
                href = link.get('href')
                if href:
                    if href.startswith('http'):
                        meeting_data['mp4_download'] = href
                    else:
                        meeting_data['mp4_download'] = urljoin('https://reflect-vod-fcgov.cablecast.tv/', href)
                    break
            
            # Alternative: look for download links in the page content
            if not meeting_data['mp4_download']:
                # Look for direct links in text
                page_text = soup.get_text()
                mp4_matches = re.findall(r'https://[^\s]*\.mp4', page_text)
                if mp4_matches:
                    meeting_data['mp4_download'] = mp4_matches[0]
            
            return meeting_data if meeting_data['title'] else None
            
        except Exception as e:
            logger.error(f"Error extracting Cablecast data: {e}")
            return None
    
    def categorize_meeting_type(self, title):
        """Categorize the type of meeting based on title"""
        title_lower = title.lower()
        
        if 'urban renewal' in title_lower:
            if 'workshop' in title_lower:
                return 'Urban Renewal Authority Workshop'
            else:
                return 'Urban Renewal Authority Board'
        elif 'regular meeting' in title_lower or 'regular' in title_lower:
            return 'City Council Regular Meeting'
        elif 'work session' in title_lower:
            return 'City Council Work Session'
        elif 'special meeting' in title_lower or 'special' in title_lower:
            return 'City Council Special Meeting'
        elif 'adjourned' in title_lower:
            return 'City Council Adjourned Meeting'
        else:
            return 'City Council Meeting'
    
    def enhance_with_additional_data(self):
        """Enhance meeting data with additional information from detail pages"""
        logger.info("Enhancing data with additional information...")
        
        for i, meeting in enumerate(self.meetings_data):
            if meeting.get('detail_page') and not meeting.get('mp4_download'):
                try:
                    response = self.fetch_page(meeting['detail_page'])
                    if response:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Look for additional video links
                        video_links = soup.find_all('a', href=re.compile(r'(video|stream|mp4|watch)', re.I))
                        for link in video_links:
                            href = link.get('href', '')
                            if 'mp4' in href:
                                meeting['mp4_download'] = urljoin(meeting['detail_page'], href)
                                break
                            elif 'cablecast' in href or 'stream' in href:
                                meeting['video_link'] = urljoin(meeting['detail_page'], href)
                        
                        time.sleep(1)  # Rate limiting
                        
                except Exception as e:
                    logger.warning(f"Error enhancing meeting data: {e}")
    
    def save_to_csv(self, filename='fort_collins_all_meetings.csv'):
        """Save the scraped data to CSV"""
        if not self.meetings_data:
            logger.warning("No meeting data to save")
            return
            
        df = pd.DataFrame(self.meetings_data)
        
        # Clean and sort data
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values('date_parsed', ascending=False, na_position='last')
        df = df.drop('date_parsed', axis=1)
        
        # Remove duplicates based on title and date
        df = df.drop_duplicates(subset=['title', 'date'], keep='first')
        
        # Save to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} meetings to {filename}")
        
        # Print detailed summary
        self.print_summary(df)
    
    def print_summary(self, df):
        """Print detailed summary of scraped data"""
        print(f"\n=== COMPREHENSIVE SCRAPING SUMMARY ===")
        print(f"Total Fort Collins City Council meetings found: {len(df)}")
        
        print(f"\nMeeting types found:")
        type_counts = df['meeting_type'].value_counts()
        for meeting_type, count in type_counts.items():
            print(f"  {meeting_type}: {count}")
        
        print(f"\nSources:")
        source_counts = df['source'].value_counts()
        for source, count in source_counts.items():
            print(f"  {source}: {count}")
        
        print(f"\nVideo availability:")
        print(f"  Meetings with MP4 downloads: {df['mp4_download'].notna().sum()}")
        print(f"  Meetings with video links: {df['video_link'].notna().sum()}")
        print(f"  Meetings with any video: {(df['mp4_download'].notna() | df['video_link'].notna()).sum()}")
        
        # Show Urban Renewal Authority count specifically
        ura_meetings = df[df['meeting_type'].str.contains('Urban Renewal', na=False)]
        print(f"  Urban Renewal Authority meetings: {len(ura_meetings)}")
        
        print(f"\nDate range:")
        dates_available = df[df['date'].notna() & (df['date'] != '')]
        if len(dates_available) > 0:
            print(f"  Earliest meeting: {dates_available['date'].min()}")
            print(f"  Latest meeting: {dates_available['date'].max()}")
        
        print(f"\nSample meetings with direct MP4 downloads:")
        mp4_meetings = df[df['mp4_download'].notna() & (df['mp4_download'] != '')]
        for _, meeting in mp4_meetings.head(10).iterrows():
            print(f"  {meeting['date']} - {meeting['title'][:60]}...")
        
        if len(mp4_meetings) > 10:
            print(f"  ... and {len(mp4_meetings) - 10} more")
        
        print(f"\nCSV saved as: {filename}")
    
    def run_comprehensive_scraper(self):
        """Run the complete comprehensive scraping process"""
        logger.info("Starting comprehensive Fort Collins City Council video scraper...")
        
        try:
            # Scrape from all sources
            logger.info("=== Phase 1: Municode Meetings Portal ===")
            self.scrape_municode_meetings()
            
            logger.info("=== Phase 2: Cablecast Video Platform ===")
            self.scrape_cablecast_videos()
            
            logger.info("=== Phase 3: Systematic Cablecast Archive Check ===")
            self.scrape_cablecast_archive_systematic()
            
            if not self.meetings_data:
                logger.error("No meeting data found from any source")
                return
            
            logger.info("=== Phase 4: Enhanced Data Collection ===")
            self.enhance_with_additional_data()
            
            logger.info("=== Phase 5: Saving Results ===")
            self.save_to_csv()
            
            logger.info("Comprehensive scraping completed successfully!")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

def main():
    """Main function"""
    try:
        scraper = FortCollinsVideoScraper()
        scraper.run_comprehensive_scraper()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")

if __name__ == "__main__":
    main()