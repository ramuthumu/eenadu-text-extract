import sqlite3

ARTICLE_DB_NAME = "articles.db"

URLS_DB_NAME="urls.db"

BASE_URL = "https://www.eenadu.net"

def setup_article_db():
    with sqlite3.connect(ARTICLE_DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            date_published TEXT,
            content TEXT
        )
        """)



def setup_urls_db():
    with sqlite3.connect(URLS_DB_NAME) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            visited BOOLEAN DEFAULT FALSE,
            scraped BOOLEAN DEFAULT FALSE
        )
        """)
        conn.execute("""
        INSERT OR IGNORE INTO urls (url, visited, scraped) VALUES (?, FALSE, FALSE)
        """, (BASE_URL,))

if __name__ == "__main__":
    setup_article_db()
    setup_urls_db()
