import json
from functools import wraps
from collections import deque
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.eenadu.net"


def retry_on_exception(max_retries=3, backoff_factor=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for i in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries:
                        raise e
                    sleep_time = backoff_factor * (2 ** i)
                    await asyncio.sleep(sleep_time)

        return wrapper

    return decorator


def save_article(article, output_file):
    with open(output_file, 'a') as f:
        json.dump(article, f, ensure_ascii=False)
        f.write('\n')


@retry_on_exception(max_retries=3, backoff_factor=1)
async def extract_content(url, session):
    async with session.get(url) as response:
        response.raise_for_status()
        content = await response.text()
        soup = BeautifulSoup(content, 'html.parser')
        fullstory_div = soup.find('div', {'class': 'fullstory'}) or soup.find('section', {'class': 'fullstory'})

        if fullstory_div:
            text = extract_text(fullstory_div)
            heading = extract_heading(fullstory_div)
            date_published = extract_date_published(fullstory_div)
        else:
            text = ''
            heading = ''
            date_published = ''

        return {'url': url, 'title': heading, 'date_published': date_published, 'content': text}, soup


def extract_text(soup):
    text = ''
    paragraphs = soup.find_all('p')
    for p in paragraphs:
        text += p.get_text() + '\n\n'
    return text


def extract_heading(soup):
    heading = ''
    heading_tag = soup.find('h1')
    if heading_tag:
        heading = heading_tag.get_text()
    return heading


def extract_date_published(soup):
    date_published = ''
    date_published_div = soup.find('div', {'class': 'pub-t'})
    if date_published_div:
        date_published = date_published_div.get_text().strip()
    return date_published


def extract_urls(soup, base_url):
    anchors = soup.find_all('a')
    return {urljoin(base_url, a.get('href')) for a in anchors if a.get('href') and a.get('href').startswith(BASE_URL)}


async def fetch_urls_in_queue(queue, visited, output_file_name, max_concurrent_requests=10):
    async with aiohttp.ClientSession() as session:
        while queue:
            tasks = []
            for i in range(min(len(queue), max_concurrent_requests)):
                current_url = queue.popleft()
                if current_url not in visited:
                    visited.add(current_url)
                    tasks.append(asyncio.ensure_future(extract_content(current_url, session)))

            articles = await asyncio.gather(*tasks, return_exceptions=True)
            for article in articles:
                if isinstance(article, tuple):  # Check if the result is a tuple (successful fetch)
                    write_article_to_file(article[0], output_file_name)
                    urls = extract_urls(article[1], BASE_URL)
                    queue.extend(url for url in urls if url not in visited)


def write_article_to_file(article, output_file_name):
    with open(output_file_name, 'a') as output_file:
        json.dump(article, output_file, ensure_ascii=False)
        output_file.write('\n') # Write each article on a new line

async def main():
    visited = set()
    queue = deque([BASE_URL])
    output_file_name = 'eenadu.json'
    await fetch_urls_in_queue(queue, visited, output_file_name)

if __name__ == "__main__":
    asyncio.run(main())
