import json
import time
from functools import wraps
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from urllib.parse import urljoin
from collections import deque

BASE_URL = "https://www.eenadu.net"

def retry_on_exception(max_retries=3, backoff_factor=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == max_retries:
                        raise e
                    sleep_time = backoff_factor * (2 ** i)
                    time.sleep(sleep_time)

        return wrapper

    return decorator


def save_article(article, output_file):
    with open(output_file, 'a') as f:
        json.dump(article, f, ensure_ascii=False)
        f.write('\n')


@retry_on_exception(max_retries=3, backoff_factor=1)
def extract_content(url, session):
    response = session.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
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


def process_url(current_url, session):
    try:
        article, soup = extract_content(current_url, session)

        save_article(article, 'eenadu.json')

        urls = extract_urls(soup, BASE_URL)
        return urls

    except requests.exceptions.RequestException as e:
        print(f"Error while requesting {current_url}: {e}")
        return set()


def main():
    with requests.Session() as session:
        visited = set()
        queue = deque([BASE_URL])

        with open('eenadu.json', 'w') as output_file:
            output_file.write('[')

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_url = {}

            while queue or future_to_url:
                if queue:
                    current_url = queue.popleft()
                    if current_url not in visited:
                        visited.add(current_url)
                        future = executor.submit(process_url, current_url, session)
                        future_to_url[future] = current_url

                for future in concurrent.futures.as_completed(list(future_to_url.keys())):
                    url = future_to_url[future]
                    try:
                        new_urls = future.result()
                        queue.extend(url for url in new_urls if url not in visited)
                    except Exception as exc:
                        print(f"{url} generated an exception: {exc}")
                    finally:
                        del future_to_url[future]

        with open('eenadu.json', 'a') as output_file:
            output_file.write(']')


if __name__ == "__main__":
    main()
