#!/usr/bin/env python3
"""
Fort Collins City Council Meeting Scraper
Scrapes meeting data from fortcollins-co.municodemeetings.com and creates a CSV database
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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FortCollinsMeetingScraper:
    def __init__(self):
        self.base_url = "https://fortcollins-co.municodemeetings.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.meetings_data = []
        
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
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def parse_main_page(self):
        """Parse the main meetings page and extract all meeting information"""
        logger.info("Fetching main meetings page...")
        response = self.fetch_page(self.base_url)
        
        if not response:
            logger.error("Failed to fetch main page")
            return
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all meeting rows in the table
        # The structure appears to be a table with meeting information
        meeting_rows = soup.find_all('tr')
        
        for row in meeting_rows:
            try:
                meeting_data = self.extract_meeting_data(row)
                if meeting_data:
                    self.meetings_data.append(meeting_data)
                    logger.info(f"Extracted: {meeting_data['title']} - {meeting_data['date']}")
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue
                
        # Also check if there are pagination or "Load More" links
        self.check_additional_pages(soup)
    
    def extract_meeting_data(self, row):
        """Extract meeting data from a table row"""
        cells = row.find_all('td')
        if len(cells) < 2:
            return None
            
        meeting_data = {
            'date': '',
            'time': '',
            'title': '',
            'meeting_type': '',
            'agenda_pdf': '',
            'agenda_html': '',
            'minutes_pdf': '',
            'minutes_html': '',
            'audio_link': '',
            'video_link': '',
            'mp4_download': '',
            'detail_page': ''
        }
        
        # Extract date and time - usually in first cell
        date_time_cell = cells[0] if cells else None
        if date_time_cell:
            date_time_text = date_time_cell.get_text(strip=True)
            # Parse date/time format like "08/19/2025 - 6:00pm"
            if ' - ' in date_time_text:
                date_part, time_part = date_time_text.split(' - ', 1)
                meeting_data['date'] = date_part.strip()
                meeting_data['time'] = time_part.strip()
        
        # Extract meeting title and type - usually in second cell
        title_cell = cells[1] if len(cells) > 1 else None
        if title_cell:
            title_text = title_cell.get_text(strip=True)
            meeting_data['title'] = title_text
            
            # Determine meeting type from title
            title_lower = title_text.lower()
            if 'city council regular' in title_lower:
                meeting_data['meeting_type'] = 'City Council Regular Meeting'
            elif 'city council work session' in title_lower:
                meeting_data['meeting_type'] = 'City Council Work Session'
            elif 'city council special' in title_lower:
                meeting_data['meeting_type'] = 'City Council Special Meeting'
            elif 'planning' in title_lower and 'zoning' in title_lower:
                meeting_data['meeting_type'] = 'Planning & Zoning Commission'
            elif 'urban renewal' in title_lower:
                meeting_data['meeting_type'] = 'Urban Renewal Authority'
            else:
                meeting_data['meeting_type'] = 'Other'
        
        # Extract document links - look for PDF, HTML, Video icons and links
        for cell in cells:
            # Find all links in the cell
            links = cell.find_all('a')
            images = cell.find_all('img')
            
            for link in links:
                href = link.get('href', '')
                if href:
                    full_url = urljoin(self.base_url, href)
                    link_text = link.get_text(strip=True).lower()
                    
                    # Check if this is a "View Details" link
                    if 'view details' in link_text or '/page/' in href:
                        meeting_data['detail_page'] = full_url
            
            # Check for document type icons
            for img in images:
                src = img.get('src', '')
                alt = img.get('alt', '').lower()
                
                # Find the associated link for this icon
                parent_link = img.find_parent('a')
                if parent_link:
                    href = parent_link.get('href', '')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        
                        if 'pdf' in src or 'pdf' in alt:
                            if not meeting_data['agenda_pdf'] and ('agenda' in href or 'agenda' in alt):
                                meeting_data['agenda_pdf'] = full_url
                            elif not meeting_data['minutes_pdf']:
                                meeting_data['minutes_pdf'] = full_url
                        elif 'html' in src or 'html' in alt:
                            if not meeting_data['agenda_html'] and ('agenda' in href or 'agenda' in alt):
                                meeting_data['agenda_html'] = full_url
                            elif not meeting_data['minutes_html']:
                                meeting_data['minutes_html'] = full_url
                        elif 'video' in src or 'video' in alt:
                            meeting_data['video_link'] = full_url
        
        # Only return if we have at least a title and date
        if meeting_data['title'] and meeting_data['date']:
            return meeting_data
        
        return None
    
    def get_video_details(self, detail_page_url):
        """Extract video download links from meeting detail page"""
        if not detail_page_url:
            return '', ''
            
        response = self.fetch_page(detail_page_url)
        if not response:
            return '', ''
            
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for direct video links or embedded video players
        video_links = soup.find_all('a', href=re.compile(r'\.mp4$|video|stream'))
        audio_links = soup.find_all('a', href=re.compile(r'\.mp3$|\.wav$|audio'))
        
        video_url = ''
        audio_url = ''
        
        if video_links:
            video_url = urljoin(self.base_url, video_links[0].get('href', ''))
        
        if audio_links:
            audio_url = urljoin(self.base_url, audio_links[0].get('href', ''))
            
        # Also check for iframe embeds (common for video players)
        iframes = soup.find_all('iframe')
        for iframe in iframes:
            src = iframe.get('src', '')
            if 'video' in src or 'stream' in src:
                video_url = src
                break
                
        return video_url, audio_url
    
    def check_additional_pages(self, soup):
        """Check for pagination or additional meeting pages"""
        # Look for pagination links
        pagination_links = soup.find_all('a', text=re.compile(r'next|more|page \d+', re.I))
        
        for link in pagination_links:
            href = link.get('href')
            if href:
                page_url = urljoin(self.base_url, href)
                logger.info(f"Found additional page: {page_url}")
                # You could recursively fetch more pages here
                # self.parse_additional_page(page_url)
    
    def enhance_with_video_data(self):
        """Enhance meeting data with video download links"""
        logger.info("Enhancing data with video information...")
        
        for i, meeting in enumerate(self.meetings_data):
            if meeting['detail_page']:
                logger.info(f"Processing detail page {i+1}/{len(self.meetings_data)}: {meeting['title']}")
                video_url, audio_url = self.get_video_details(meeting['detail_page'])
                
                if video_url:
                    meeting['mp4_download'] = video_url
                if audio_url:
                    meeting['audio_link'] = audio_url
                    
                # Rate limiting
                time.sleep(1)
    
    def save_to_csv(self, filename='fort_collins_meetings.csv'):
        """Save the scraped data to CSV"""
        if not self.meetings_data:
            logger.warning("No meeting data to save")
            return
            
        df = pd.DataFrame(self.meetings_data)
        
        # Sort by date (newest first)
        df['date_parsed'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.sort_values('date_parsed', ascending=False)
        df = df.drop('date_parsed', axis=1)
        
        # Save to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} meetings to {filename}")
        
        # Print summary
        print(f"\n=== SCRAPING SUMMARY ===")
        print(f"Total meetings found: {len(df)}")
        print(f"Meeting types:")
        print(df['meeting_type'].value_counts())
        print(f"\nFiles with video links: {df['video_link'].notna().sum()}")
        print(f"Files with MP4 downloads: {df['mp4_download'].notna().sum()}")
        print(f"CSV saved as: {filename}")
    
    def run_scraper(self):
        """Run the complete scraping process"""
        logger.info("Starting Fort Collins meeting scraper...")
        
        try:
            # Parse main page
            self.parse_main_page()
            
            if not self.meetings_data:
                logger.error("No meeting data found")
                return
            
            # Enhance with video data
            self.enhance_with_video_data()
            
            # Save to CSV
            self.save_to_csv()
            
            logger.info("Scraping completed successfully!")
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise

def main():
    """Main function"""
    try:
        scraper = FortCollinsMeetingScraper()
        scraper.run_scraper()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")

if __name__ == "__main__":
    main()
