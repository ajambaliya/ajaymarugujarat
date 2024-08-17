import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin, urlparse
from telegram import Bot
import asyncio
import pyshorteners
import time
from pymongo import MongoClient
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Telegram bot token and channel ID
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')

# Initialize URL shortener
shortener = pyshorteners.Shortener()

# MongoDB configuration
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Create a session with retry strategy
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

def make_request(url):
    logger.info(f"Attempting to make request to {url}")
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully made request to {url}")
        return response
    except requests.exceptions.SSLError as ssl_err:
        logger.error(f"SSL Error when accessing {url}: {ssl_err}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error accessing {url}: {e}")
        return None

async def send_to_telegram(message, file=None):
    logger.info("Attempting to send message to Telegram")
    bot = Bot(token=BOT_TOKEN)
    
    try:
        if file:
            async with bot:
                await bot.send_document(chat_id=CHANNEL_ID, document=open(file, 'rb'), caption=message)
        else:
            async with bot:
                await bot.send_message(chat_id=CHANNEL_ID, text=message)
        logger.info("Successfully sent message to Telegram")
    except Exception as e:
        logger.error(f"Error sending message to Telegram: {e}")

def download_and_verify_file(url):
    logger.info(f"Attempting to download file from {url}")
    try:
        response = make_request(url)
        if response is None:
            return None
        
        content_type = response.headers.get('Content-Type')
        if 'pdf' in content_type:
            extension = '.pdf'
        elif 'image' in content_type:
            extension = '.' + content_type.split('/')[1]
        else:
            parsed_url = urlparse(url)
            path = parsed_url.path
            extension = os.path.splitext(path)[1]
            if not extension:
                extension = '.pdf'
        
        filename = os.path.basename(urlparse(url).path) or 'download'
        filename += extension
        
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        
        if os.path.getsize(filename) > 0:
            logger.info(f"File downloaded and verified: {filename}")
            return filename
        else:
            logger.warning(f"Downloaded file is empty: {filename}")
            os.remove(filename)
            return None

    except Exception as e:
        logger.error(f"Error downloading file from {url}: {e}")
        return None

def shorten_url(url):
    logger.info(f"Attempting to shorten URL: {url}")
    time.sleep(2)
    try:
        short_url = shortener.tinyurl.short(url)
        logger.info(f"Successfully shortened URL: {url} to {short_url}")
        return short_url
    except Exception as e:
        logger.error(f"URL shortening failed for {url}: {e}")
        return url

def fetch_urls():
    logger.info("Fetching URLs from marugujarat.in")
    base_url = 'https://www.marugujarat.in/'
    response = make_request(base_url)
    if response is None:
        logger.error("Failed to fetch URLs from marugujarat.in")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', class_='_self cvplbd')
    
    urls = [urljoin(base_url, link['href']) for link in links]
    logger.info(f"Found {len(urls)} URLs")
    return urls

def scrape_selected_url(url):
    logger.info(f"Scraping URL: {url}")
    response = make_request(url)
    if response is None:
        return None, None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    title_tag = soup.find('h1', class_='entry-title')
    if title_tag is None:
        logger.error(f"Unable to find the job title on the page: {url}")
        return None, None
    
    title = title_tag.text.strip()
    
    job_details = {}
    blockquotes = soup.find_all('blockquote', class_='style-3')
    for blockquote in blockquotes:
        links = blockquote.find_all('a')
        for link in links:
            text = link.find_previous('b').text.strip(':')
            if any(keyword in text for keyword in ['Job Advertisement', 'Official website', 'Apply Online', 'Job Notification']):
                job_details[text] = link['href']
    
    logger.info(f"Successfully scraped URL: {url}")
    return title, job_details

async def handle_files_and_send_to_telegram(title, job_details):
    if title is None:
        logger.warning("Skipping due to missing title.")
        return
    
    logger.info(f"Handling files and sending to Telegram for job: {title}")
    message = f"📢 {title} 📢\n\n"
    
    job_notification_file = None
    other_files = []
    
    for key, url in job_details.items():
        short_url = shorten_url(url)
        
        if 'Job Advertisement' in key:
            message += f"📝 Job Advertisement: {short_url}\n"
        elif 'Job Notification' in key:
            message += f"📄 Job Notification: {short_url}\n"
        elif 'Official website' in key:
            message += f"🌐 Official Website: {short_url}\n"
        elif 'Apply Online' in key:
            message += f"🖥️ Apply Online: {short_url}\n"
        else:
            message += f"🔗 {key}: {short_url}\n"
        
        file_path = download_and_verify_file(url)
        if file_path:
            if 'Job Notification' in key:
                job_notification_file = file_path
            else:
                other_files.append(file_path)
    
    promo_message = "\n🚀 આવી જ તમામ જોબ અપડેટ રેગ્યુલર કોઇ પણ એડ વગર જોવા માટે અમારા ચેનલમાં જોડાઇ જાવ ! 🚀\n👉 https://t.me/currentadda 👈"
    message += promo_message
    
    file_to_send = job_notification_file if job_notification_file else (other_files[0] if other_files else None)
    
    if file_to_send:
        await send_to_telegram(message, file=file_to_send)
    else:
        await send_to_telegram(message)

def is_url_scraped(url):
    result = collection.find_one({"url": url}) is not None
    logger.info(f"Checking if URL is scraped: {url} - Result: {result}")
    return result

def mark_url_as_scraped(url, title):
    logger.info(f"Marking URL as scraped: {url}")
    collection.insert_one({"url": url, "title": title, "scraped_at": time.time()})

async def scrape_and_send(url):
    if is_url_scraped(url):
        logger.info(f"URL already scraped: {url}")
        return

    logger.info(f"Scraping and sending for URL: {url}")
    title, job_details = scrape_selected_url(url)
    if title:
        await handle_files_and_send_to_telegram(title, job_details)
        mark_url_as_scraped(url, title)
    else:
        logger.error(f"Failed to scrape URL: {url}")

def get_unscraped_urls(urls):
    unscraped = [url for url in urls if not is_url_scraped(url)]
    logger.info(f"Found {len(unscraped)} unscraped URLs out of {len(urls)} total URLs")
    return unscraped

async def main():
    logger.info("Starting main function")
    urls = fetch_urls()
    unscraped_urls = get_unscraped_urls(urls)
    
    logger.info(f"Found {len(unscraped_urls)} unscraped URLs.")
    
    for i, url in enumerate(unscraped_urls, 1):
        logger.info(f"Scraping URL {i}/{len(unscraped_urls)}: {url}")
        await scrape_and_send(url)
        time.sleep(10)
    
    logger.info("Finished scraping all new URLs.")

if __name__ == '__main__':
    logger.info("Script started")
    asyncio.run(main())
    logger.info("Script completed")
