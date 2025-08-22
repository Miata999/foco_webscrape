# Fort Collins City Council Meeting Scraper & Downloader

A comprehensive Python solution for scraping Fort Collins city council meeting data and downloading associated videos, audio, and documents from the official meeting portal.

## Overview

This project consists of two main components:

1. **Meeting Scraper** (`fc_meeting_scraper.py`) - Extracts meeting data from [fortcollins-co.municodemeetings.com](https://fortcollins-co.municodemeetings.com) and creates a CSV database
2. **Video Downloader** (`fc_video_downloader.py`) - Downloads videos, audio, and documents based on the CSV data

## Features

- **Comprehensive Data Extraction**: Scrapes meeting dates, times, titles, types, and all associated document links
- **Multiple File Types**: Supports downloading videos (MP4), audio files, PDF agendas, and minutes
- **Organized Downloads**: Automatically organizes files into separate directories (videos, audio, documents)
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Error Handling**: Robust error handling with retry logic and failed download tracking
- **Filtering Options**: Filter downloads by meeting type, date range, or limit number of meetings
- **Resume Capability**: Skips already downloaded files

## Installation

### Prerequisites

- Python 3.7 or higher
- pip (Python package installer)

### Setup

1. Clone the repository:
```bash
git clone git@github.com:Miata999/foco_webscrape.git
cd foco_webscrape
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Step 1: Scrape Meeting Data

Run the scraper to extract meeting information and create a CSV database:

```bash
python fc_meeting_scraper.py
```

This will:
- Scrape all available meetings from the Fort Collins meeting portal
- Extract meeting metadata (date, time, title, type)
- Find links to videos, audio, agendas, and minutes
- Save everything to `fort_collins_meetings.csv`

### Step 2: Download Files

Use the downloader to get the actual media files and documents:

```bash
# Download all available files
python fc_video_downloader.py

# Download only specific meeting types
python fc_video_downloader.py --types "City Council Regular Meeting" "City Council Work Session"

# Limit to recent meetings
python fc_video_downloader.py --limit 10

# Download only videos (skip audio and documents)
python fc_video_downloader.py --no-audio --no-docs

# Use custom CSV file and output directory
python fc_video_downloader.py --csv my_meetings.csv --output my_downloads
```

## Command Line Options

### Video Downloader Options

- `--csv`: Specify CSV file (default: `fort_collins_meetings.csv`)
- `--output`: Output directory for downloads (default: `downloads`)
- `--types`: Filter by meeting types (e.g., "City Council Regular Meeting")
- `--limit`: Limit number of meetings to process
- `--no-videos`: Skip video downloads
- `--no-audio`: Skip audio downloads
- `--no-docs`: Skip document downloads

## Output Structure

After running the downloader, files will be organized as follows:

```
downloads/
├── videos/          # MP4 video files
├── audio/           # Audio files (MP3, WAV)
└── documents/       # PDF agendas and minutes
```

## Meeting Types

The scraper identifies and categorizes different meeting types:

- City Council Regular Meeting
- City Council Work Session
- City Council Special Meeting
- Planning & Zoning Commission
- Urban Renewal Authority
- Other

## Data Fields

The CSV file contains the following columns:

- `date`: Meeting date
- `time`: Meeting time
- `title`: Meeting title
- `meeting_type`: Categorized meeting type
- `agenda_pdf`: Link to agenda PDF
- `agenda_html`: Link to agenda HTML
- `minutes_pdf`: Link to minutes PDF
- `minutes_html`: Link to minutes HTML
- `audio_link`: Link to audio file
- `video_link`: Link to video stream
- `mp4_download`: Direct MP4 download link
- `detail_page`: Link to meeting detail page

## Examples

### Download Recent City Council Meetings

```bash
# Scrape all meetings
python fc_meeting_scraper.py

# Download only the 5 most recent City Council Regular Meetings
python fc_video_downloader.py --types "City Council Regular Meeting" --limit 5
```

### Download All Available Content

```bash
# Download everything for all meetings
python fc_video_downloader.py
```

### Custom Organization

```bash
# Download to a custom directory with only videos and documents
python fc_video_downloader.py --output "fort_collins_archive" --no-audio
```

## Error Handling

The scripts include comprehensive error handling:

- **Network Issues**: Automatic retry with exponential backoff
- **Missing Files**: Graceful handling of broken links
- **Rate Limiting**: Built-in delays to avoid overwhelming the server
- **Partial Downloads**: Cleanup of incomplete files
- **Detailed Logging**: Comprehensive logging for debugging

## Troubleshooting

### Common Issues

1. **No meetings found**: Check your internet connection and verify the meeting portal is accessible
2. **Download failures**: Some files may be temporarily unavailable or require authentication
3. **Permission errors**: Ensure you have write permissions in the output directory

### Logs

Both scripts provide detailed logging. Check the console output for:
- Scraping progress and statistics
- Download progress and file sizes
- Error messages and failed downloads
- Summary reports

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the scraper.

## Legal Notice

This tool is for educational and research purposes. Please respect the website's terms of service and robots.txt file. Use responsibly and avoid overwhelming the server with requests.

## License

This project is open source. Please use responsibly and in accordance with the target website's terms of service.
