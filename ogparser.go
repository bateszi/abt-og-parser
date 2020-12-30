package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type AppConfig struct {
	Db DbConfig `json:"db"`
	Solr string `json:"solr"`
}

type DbConfig struct {
	User string `json:"user"`
	Password string `json:"pass"`
	Server string `json:"server"`
	DbName string `json:"dbName"`
}

type Post struct {
	PostID int64
	Url string
}

type PostScraped struct {
	Post Post
	Html string
	OpenGraphTags OpenGraphTags
} 

type OpenGraphTags struct {
	Description string
	FeaturedImage string
}

type AbtSolrDocs []AbtSolrDocument

type AbtSolrDocument struct {
	Id int64 `json:"id"`
	PostDescription SolrSetDocument `json:"post_description"`
}

type SolrSetDocument struct {
	Set string `json:"set"`
}

var scrapingPostsWg sync.WaitGroup

func kill(context string, err error) {
	fmt.Println("error encountered with reason:", context)
	panic(err)
}

func updateSolr(solrBaseUrl string, scraped PostScraped) {
	docs := AbtSolrDocs{
		AbtSolrDocument{
			Id: scraped.Post.PostID,
			PostDescription: SolrSetDocument{
				Set: scraped.OpenGraphTags.Description,
			},
		},
	}

	postBody, err := json.Marshal(docs)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	solrUrl := solrBaseUrl + "/update?commit=true"
	req, err := http.NewRequest("POST", solrUrl, bytes.NewBuffer(postBody))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	req.Header.Set("Content-Type", "application/json")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	req = req.WithContext(ctx)

	httpClient := &http.Client{}

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)
}

func updateDbWithOgTags(db *sql.DB, scraped PostScraped) {
	stmt, err := db.Prepare("UPDATE posts SET description = ?, modified = ?, content = ? WHERE pk_post_id = ?")
	if err != nil {
		fmt.Println(
			"Could not prepare SQL statement to update post with og values", scraped.Post.Url, err.Error(),
		)
		return
	}
	_, err = stmt.Exec(
		scraped.OpenGraphTags.Description,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		scraped.Html,
		scraped.Post.PostID,
	)
	if err != nil {
		fmt.Println(
			"Could not execute SQL statement to update post with og values", scraped.Post.Url, err.Error(),
		)
		return
	}

	var ttlFiles float64
	err = db.QueryRow("SELECT COUNT(*) AS ttl FROM files WHERE fk_post_id = ?", scraped.Post.PostID).Scan(&ttlFiles)
	if err != nil && err.Error() != "sql: no rows in result set" {
		fmt.Println("could not count files", err.Error())
		return
	}
	if ttlFiles == 0 {
		stmt, err = db.Prepare("INSERT INTO `files` (`fk_post_id`, `external_url`) VALUES (?, ?)")
		if err != nil {
			fmt.Println(
				"Could not prepare SQL statement to insert post image", scraped.Post.Url, err.Error(),
			)
			return
		}
		_, err = stmt.Exec(
			scraped.Post.PostID,
			scraped.OpenGraphTags.FeaturedImage,
		)
		if err != nil {
			fmt.Println(
				"Could not execute SQL statement to insert post image", scraped.Post.Url, err.Error(),
			)
			return
		}
	}
}

func getOgTagsFromHtml(scrapedPost *PostScraped) {
	r := strings.NewReader(scrapedPost.Html)
	tokenizer := html.NewTokenizer(r)

	for {
		tokenType := tokenizer.Next()

		if tokenType == html.ErrorToken {
			err := tokenizer.Err()
			if err == io.EOF {
				break
			}
		}

		token := tokenizer.Token()

		if token.Data == "meta" {
			isDescr := false
			isThumb := false
			for i := range token.Attr {
				if token.Attr[i].Key == "property" && token.Attr[i].Val == "og:description" {
					isDescr = true
					break
				} else if token.Attr[i].Key == "property" && token.Attr[i].Val == "og:image" {
					isThumb = true
					break
				}
			}

			for j := range token.Attr {
				if token.Attr[j].Key == "content" {
					if isDescr {
						scrapedPost.OpenGraphTags.Description = token.Attr[j].Val
					} else if isThumb {
						scrapedPost.OpenGraphTags.FeaturedImage = token.Attr[j].Val
					}
				}
			}
		}
	}
}

func getPostHtml(post Post, scrapedChan chan<- PostScraped) {
	fmt.Println("fetching", post.Url)

	scrapedPost := PostScraped{
		Post:          post,
		Html:          "",
		OpenGraphTags: OpenGraphTags{},
	}

	defer func() {
		scrapedChan <- scrapedPost
		scrapingPostsWg.Done()
	}()

	req, err := http.NewRequest("GET", post.Url, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// tumblr gdpr nonsense
	if !strings.Contains(post.Url, "tumblr.com") {
		req.Header.Add("User-Agent", "@bateszi OG parser")
	} else {
		req.Header.Add("User-Agent", "Baiduspider")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)

	defer func(cancel context.CancelFunc) {
		cancel()
	}(cancel)

	req = req.WithContext(ctx)

	httpClient := &http.Client{}

	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func(resp *http.Response) {
		_ = resp.Body.Close()
	}(resp)

	if resp.StatusCode == http.StatusOK && resp.StatusCode < 300 {
		httpBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		scrapedPost.Html = string(httpBody)
	}
}

func getPostsToScrape(db *sql.DB) ([]Post, error) {
	posts := make([]Post, 0)

	getPostsRows, err := db.Query(
		"SELECT pk_post_id, link FROM rss_aggregator.posts WHERE created > (NOW() - interval 60 minute)",
	)
	if err != nil {
		return posts, err
	}

	defer func(getFeedsRows *sql.Rows) {
		err := getFeedsRows.Close()
		if err != nil {
			panic(err)
		}
	}(getPostsRows)

	for getPostsRows.Next() {
		post := Post{}
		err = getPostsRows.Scan(
			&post.PostID,
			&post.Url,
		)
		if err != nil {
			return posts, err
		}

		posts = append(posts, post)
	}
	
	return posts, nil
}

func start() {
	// recover from panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	// read the json config
	encodedJson, err := ioutil.ReadFile("config/config.json")
	if err != nil {
		kill("reading config file", err)
	}

	config := AppConfig{}

	err = json.Unmarshal(encodedJson, &config)
	if err != nil {
		kill("parsing json from config file", err)
	}

	// create a db connection
	dbParams := make(map[string]string)
	dbParams["charset"] = "utf8mb4"

	dbConfig := mysql.Config{
		User: config.Db.User,
		Passwd: config.Db.Password,
		Net: "tcp",
		Addr: config.Db.Server,
		DBName: config.Db.DbName,
		Params: dbParams,
	}

	db, err := sql.Open("mysql", dbConfig.FormatDSN())
	if err != nil {
		kill("opening db connection", err)
	}

	defer func(db *sql.DB) {
		fmt.Println("Closing database connection at", time.Now().Format(time.RFC1123Z))
		err := db.Close()
		if err != nil {
			kill("closing db connection", err)
		}
	}(db)

	err = db.Ping()
	if err != nil {
		kill("could not ping db", err)
	}

	fmt.Println("Opened database connection at", time.Now().Format(time.RFC1123Z))

	// get the posts to be scraped
	posts, err := getPostsToScrape(db)
	if err != nil {
		kill("fetching posts to scrape", err)
	}

	scrapedChan := make(chan PostScraped, len(posts))

	for i := range posts {
		scrapingPostsWg.Add(1)
		go getPostHtml(posts[i], scrapedChan)
	}

	scrapingPostsWg.Wait()
	fmt.Println("finished scraping posts")
	close(scrapedChan)

	for j := 0; j < len(posts); j++ {
		scrapedPost := <-scrapedChan

		fmt.Println("parsing html returned from", scrapedPost.Post.Url)

		getOgTagsFromHtml(&scrapedPost)

		if scrapedPost.OpenGraphTags.FeaturedImage != "" && scrapedPost.OpenGraphTags.Description != "" {
			fmt.Println("updating OG tags parsed from", scrapedPost.Post.Url)
			updateDbWithOgTags(db, scrapedPost)
			updateSolr(config.Solr, scrapedPost)
		}
	}
}

func main() {
	start()

	interval := 7 * time.Minute
	fmt.Println("Starting ticker to parse posts every", interval)

	ticker := time.NewTicker(interval)

	for _ = range ticker.C {
		start()
	}

	// Run application indefinitely
	select{}
}