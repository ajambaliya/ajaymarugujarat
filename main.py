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

# Function to make requests with error handling
def make_request(url):
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response
    except requests.exceptions.SSLError as ssl_err:
        print(f"SSL Error when accessing {url}: {ssl_err}")
        print("Please check the SSL certificate of the website.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error accessing {url}: {e}")
        return None

# Function to send a message to Telegram
async def send_to_telegram(message, file=None):
    bot = Bot(token=BOT_TOKEN)
    
    if file:
        async with bot:
            await bot.send_document(chat_id=CHANNEL_ID, document=open(file, 'rb'), caption=message)
    else:
        async with bot:
            await bot.send_message(chat_id=CHANNEL_ID, text=message)

# Function to download and verify file
def download_and_verify_file(url):
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
        
        # Verify file integrity
        if os.path.getsize(filename) > 0:
            print(f"File downloaded and verified: {filename}")
            return filename
        else:
            print(f"Downloaded file is empty: {filename}")
            os.remove(filename)
            return None

    except Exception as e:
        print(f"Error downloading file from {url}: {e}")
        return None

# Shorten URLs with delay
def shorten_url(url):
    time.sleep(2)  # Add delay for URL shortening
    try:
        return shortener.tinyurl.short(url)
    except Exception as e:
        print(f"URL shortening failed for {url}: {e}")
        return url

# Fetch and display URLs from marugujarat.in
def fetch_urls():
    base_url = 'https://www.marugujarat.in/'
    response = make_request(base_url)
    if response is None:
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    links = soup.find_all('a', class_='_self cvplbd')
    
    urls = []
    for link in links:
        url = urljoin(base_url, link['href'])
        urls.append(url)
    
    return urls

# Scrape the selected URL
def scrape_selected_url(url):
    response = make_request(url)
    if response is None:
        return None, None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    title_tag = soup.find('h1', class_='entry-title')
    if title_tag is None:
        print(f"Error: Unable to find the job title on the page: {url}")
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
    
    return title, job_details

# Handle files and send to Telegram
async def handle_files_and_send_to_telegram(title, job_details):
    if title is None:
        print("Skipping due to missing title.")
        return
    
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
    return collection.find_one({"url": url}) is not None

def mark_url_as_scraped(url, title):
    collection.insert_one({"url": url, "title": title, "scraped_at": time.time()})

async def scrape_and_send(url):
    if is_url_scraped(url):
        print(f"URL already scraped: {url}")
        return

    title, job_details = scrape_selected_url(url)
    if title:
        await handle_files_and_send_to_telegram(title, job_details)
        mark_url_as_scraped(url, title)
    else:
        print(f"Failed to scrape URL: {url}")

# Function to get unscraped URLs
def get_unscraped_urls(urls):
    unscraped = []
    for url in urls:
        if not is_url_scraped(url):
            unscraped.append(url)
    return unscraped

# Main function
async def main():
    urls = fetch_urls()
    unscraped_urls = get_unscraped_urls(urls)
    
    print(f"Found {len(unscraped_urls)} unscraped URLs.")
    
    for i, url in enumerate(unscraped_urls, 1):
        print(f"Scraping URL {i}/{len(unscraped_urls)}: {url}")
        await scrape_and_send(url)
        time.sleep(10)  # 10-second delay between scrapes
    
    print("Finished scraping all new URLs.")

# Run the async main function
if __name__ == '__main__':
    asyncio.run(main())
