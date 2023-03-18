import json
import requests
import time
from functools import wraps
from bs4 import BeautifulSoup

# URL to scrape
url = "https://www.eenadu.net"


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


@retry_on_exception()
def extract_content(url, session):
    response = session.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')
    fullstory_div = soup.find('div', {'class': 'fullstory'})
    text = ''
    heading = ''
    date_published = ''
    if fullstory_div:
        heading = fullstory_div.find('h1')
        if heading:
            heading = heading.get_text()
        date_published_div = fullstory_div.find('div', {'class': 'eng-body grey pub-t'})
        if date_published_div:
            date_published = date_published_div.get_text().strip()
        text_div = fullstory_div.find('div', {'class': 'text-justify'})
        if text_div:
            paragraphs = text_div.find_all('p')
            for p in paragraphs:
                text += p.get_text() + '\n\n'
    return {'url': url, 'title': heading, 'date_published': date_published, 'content': text}, soup


def extract_urls(soup):
    anchors = soup.find_all('a')
    return {a.get('href') for a in anchors if a.get('href') and a.get('href').startswith("https://www.eenadu.net/")}


articles = []

with requests.Session() as session:
    visited = set()

    # Extract main content
    article, main_soup = extract_content(url, session)
    articles.append(article)
    visited.add(url)

    # Extract URLs
    urls = extract_urls(main_soup)

    # Loop through each URL and extract the text content from the specified elements
    for url in urls - visited:
        visited.add(url)
        try:
            article, soup = extract_content(url, session)
            articles.append(article)

            # Find all the anchor tags on the page and extract the URLs
            link_urls = extract_urls(soup)

            # Loop through each link URL and extract the text content
            for link_url in link_urls - visited:
                visited.add(link_url)
                link_article, _ = extract_content(link_url, session)
                articles.append(link_article)

        except requests.exceptions.RequestException as e:
            print(f"Error while requesting {url}: {e}")
            continue

# Save the articles to a JSON file
with open('eenadu.json', 'w') as output_file:
    json.dump(articles, output_file, ensure_ascii=False, indent=2)
