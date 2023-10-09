import json
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from urllib.parse import urljoin
import sqlite3

BASE_URL = "https://www.eenadu.net"

URLS_DB_NAME = "urls.db"
ARTICLE_DB_NAME = "articles.db"

BATCH_SIZE = 100  # Number of URLs to process in a batch


def get_next_urls(batch_size):
    with sqlite3.connect(URLS_DB_NAME) as conn:
        return [row[0] for row in conn.execute("""
        SELECT url FROM urls WHERE visited = FALSE LIMIT ?
        """, (batch_size,))]


def mark_urls_as_visited(urls):
    with sqlite3.connect(URLS_DB_NAME) as conn:
        conn.executemany("""
        UPDATE urls SET visited = TRUE WHERE url = ?
        """, [(url,) for url in urls])


def mark_urls_as_scraped(urls):
    with sqlite3.connect(URLS_DB_NAME) as conn:
        conn.executemany("""
        UPDATE urls SET scraped = TRUE WHERE url = ?
        """, [(url,) for url in urls])


def insert_new_urls(urls):
    with sqlite3.connect(URLS_DB_NAME) as conn:
        conn.executemany("""
        INSERT OR IGNORE INTO urls (url, visited, scraped) VALUES (?, FALSE, FALSE)
        """, [(url,) for url in urls])


def save_article(article, output_file):
    with open(output_file, 'a') as f:
        json.dump(article, f, ensure_ascii=False)
        f.write('\n')


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
    return {urljoin(base_url, a.get('href')) for a in anchors if a.get('href') and a.get('href').startswith(base_url)}


def insert_article(article):
    with sqlite3.connect(ARTICLE_DB_NAME) as conn:
        conn.execute("""
        INSERT INTO articles (url, title, date_published, content) VALUES (?, ?, ?, ?)
        """, (article['url'], article['title'], article['date_published'], article['content']))


def process_url(current_url, session):
    try:
        article, soup = extract_content(current_url, session)

        # Save the article directly to the database instead of a JSON file
        insert_article(article)

        urls = extract_urls(soup, BASE_URL)
        return urls

    except requests.exceptions.RequestException as e:
        print(f"Error while requesting {current_url}: {e}")
        return set()


def main():

    with requests.Session() as session:

        with concurrent.futures.ThreadPoolExecutor() as executor:
            while True:
                current_urls = get_next_urls(BATCH_SIZE)
                if not current_urls:
                    break

                mark_urls_as_visited(current_urls)

                future_to_url = {
                    executor.submit(process_url, url, session): url for url in current_urls
                }

                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        new_urls = future.result()
                        insert_new_urls(new_urls)
                    except Exception as exc:
                        print(f"{url} generated an exception: {exc}")

                mark_urls_as_scraped(current_urls)


if __name__ == "__main__":
    main()
