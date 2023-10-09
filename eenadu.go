package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/mattn/go-sqlite3"
)

const (
	baseURL       = "https://www.eenadu.net"
	urlsDBName    = "urls.db"
	articleDBName = "articles.db"
	batchSize     = 100
)

type Article struct {
	URL           string `json:"url"`
	Title         string `json:"title"`
	DatePublished string `json:"date_published"`
	Content       string `json:"content"`
}

func getNextURLs(batchSize int) []string {
	db, err := sql.Open("sqlite3", urlsDBName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT url FROM urls WHERE visited = FALSE LIMIT ?", batchSize)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			log.Fatal(err)
		}
		urls = append(urls, url)
	}

	return urls
}

func markURLsAsVisited(urls []string) {
	db, err := sql.Open("sqlite3", urlsDBName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for _, url := range urls {
		_, err := tx.Exec("UPDATE urls SET visited = TRUE WHERE url = ?", url)
		if err != nil {
			log.Printf("Failed to mark URL as visited: %s", url)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func insertNewURLs(urls []string) {
	db, err := sql.Open("sqlite3", urlsDBName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO urls (url, visited, scraped) VALUES (?, FALSE, FALSE)")
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range urls {
		_, err := stmt.Exec(u)
		if err != nil {
			log.Printf("Failed to insert URL: %s", u)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func insertArticle(article Article) {
	db, err := sql.Open("sqlite3", articleDBName)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO articles (url, title, date_published, content) VALUES (?, ?, ?, ?)",
		article.URL, article.Title, article.DatePublished, article.Content)
	if err != nil {
		log.Printf("Failed to insert article for URL: %s", article.URL)
	}
}

func extractContent(u string) (Article, *goquery.Document, error) {
	resp, err := http.Get(u)
	if err != nil {
		return Article{}, nil, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return Article{}, nil, err
	}

	fullstorySelection := doc.Find("div.fullstory, section.fullstory")
	title := extractHeading(fullstorySelection)
	content := extractText(fullstorySelection)
	datePublished := extractDatePublished(fullstorySelection)

	return Article{
		URL:           u,
		Title:         title,
		DatePublished: datePublished,
		Content:       content,
	}, doc, nil
}

func extractText(s *goquery.Selection) string {
	var text string
	s.Find("p").Each(func(i int, p *goquery.Selection) {
		text += p.Text() + "\n\n"
	})
	return text
}

func extractHeading(s *goquery.Selection) string {
	return s.Find("h1").Text()
}

func extractDatePublished(s *goquery.Selection) string {
	return s.Find("div.pub-t").Text()
}

func extractURLs(doc *goquery.Document) []string {
	var urls []string
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists && strings.HasPrefix(href, baseURL) {
			urls = append(urls, href)
		}
	})
	return urls
}

func processURL(u string, wg *sync.WaitGroup) {
	defer wg.Done()

	article, doc, err := extractContent(u)
	if err != nil {
		log.Printf("Error while requesting %s: %s\n", u, err)
		return
	}

	insertArticle(article)
	newURLs := extractURLs(doc)
	insertNewURLs(newURLs)
}

func main() {
	for {
		currentURLs := getNextURLs(batchSize)
		if len(currentURLs) == 0 {
			fmt.Println("No more URLs to process. Exiting.")
			break
		}

		markURLsAsVisited(currentURLs)

		var wg sync.WaitGroup
		for _, u := range currentURLs {
			wg.Add(1)
			go processURL(u, &wg)
		}
		wg.Wait()

		time.Sleep(1 * time.Second) // Optional: to avoid hitting the server too hard
	}
}
