package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Flags
var (
	inputDir    = flag.String("inputDir", "vine_tweets", "Directory containing Vine-Tweets text files")
	outDir      = flag.String("outDir", "vine_archive_harvest", "Output root directory")
	baseProfile = flag.String("baseProfile", "https://archive.vine.co/profiles", "Base URL for profile JSON (no trailing slash)")
	basePost    = flag.String("basePost", "https://archive.vine.co/posts", "Base URL for post JSON (no trailing slash)")
	workers     = flag.Int("workers", 128, "Number of concurrent workers")
	download    = flag.Bool("download", false, "Download media files from vines.s3.amazonaws.com")
)

// HTTP client (shared)
var httpClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	},
}

// global rate limiter
// Tweak this if you want to push harder, e.g. time.Second/10 ≈ 10 req/s
var rateLimiter = time.Tick(time.Second / 200)

// downloadedMedia keeps us from downloading the same file more than once.
var downloadedMedia = struct {
	mu sync.Mutex
	m  map[string]struct{}
}{m: make(map[string]struct{})}

// regex to extract vine.co/v/<id> slugs
var vineURLRe = regexp.MustCompile(`vine\.co\/v\/([A-Za-z0-9]+)`)

func main() {
	flag.Parse()

	profilesDir := filepath.Join(*outDir, "profiles")
	postsRoot := filepath.Join(*outDir, "posts")
	mediaRoot := filepath.Join(*outDir, "media")

	if err := os.MkdirAll(profilesDir, 0755); err != nil {
		log.Fatalf("MkdirAll profilesDir: %v", err)
	}
	if err := os.MkdirAll(postsRoot, 0755); err != nil {
		log.Fatalf("MkdirAll postsRoot: %v", err)
	}
	if *download {
		if err := os.MkdirAll(mediaRoot, 0755); err != nil {
			log.Fatalf("MkdirAll mediaRoot: %v", err)
		}
	}

	// Step 1: scan vine_tweets for vine.co/v/... slugs
	log.Printf("=== Scanning %s for Vine video URLs ===\n", *inputDir)
	slugs, err := collectVineSlugs(*inputDir)
	if err != nil {
		log.Fatalf("collectVineSlugs: %v", err)
	}
	if len(slugs) == 0 {
		log.Fatalf("No Vine video URLs found in %s", *inputDir)
	}
	log.Printf("Collected %d unique Vine video IDs from %s\n", len(slugs), *inputDir)

	// Step 2: from those slugs, fetch posts + discover user IDs
	log.Println("=== Seeding posts and discovering users from slugs ===")
	userIDs, err := fetchUsersFromSlugs(slugs, postsRoot)
	if err != nil {
		log.Fatalf("fetchUsersFromSlugs: %v", err)
	}
	if len(userIDs) == 0 {
		log.Fatalf("No user IDs discovered from Vine tweets")
	}
	log.Printf("Discovered %d unique user IDs from vine_tweets\n", len(userIDs))

	// Save profiles.json with discovered user IDs (for reuse/debug)
	profilesJSONPath := filepath.Join(*outDir, "profiles.json")
	if err := writeJSONFile(profilesJSONPath, userIDs); err != nil {
		log.Printf("Warning: failed to write %s: %v\n", profilesJSONPath, err)
	} else {
		log.Printf("Wrote discovered user IDs to %s\n", profilesJSONPath)
	}

	// Step 3: for each user, fetch profile + all posts from profile
	log.Println("=== Harvesting profiles + posts per user ===")

	jobs := make(chan string, *workers*2)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for uid := range jobs {
				if err := processUser(uid, profilesDir, postsRoot, mediaRoot, workerID); err != nil {
					log.Printf("[worker %d] user %s: %v\n", workerID, uid, err)
				}
			}
		}(i)
	}

	for _, uid := range userIDs {
		jobs <- uid
	}
	close(jobs)
	wg.Wait()

	log.Println("All done.")
}

// ------------------------ Step 1: scan vine_tweets for slugs ------------------------

func collectVineSlugs(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}

	slugSet := make(map[string]struct{})

	err = filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			// skip this entry
			return nil
		}
		if fi.IsDir() {
			return nil
		}

		// You can filter by extension if you want, e.g. only .txt
		// if !strings.HasSuffix(strings.ToLower(fi.Name()), ".txt") { return nil }

		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			matches := vineURLRe.FindAllStringSubmatch(line, -1)
			for _, m := range matches {
				if len(m) >= 2 {
					slug := strings.TrimSpace(m[1])
					if slug != "" {
						slugSet[slug] = struct{}{}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slugs := make([]string, 0, len(slugSet))
	for s := range slugSet {
		slugs = append(slugs, s)
	}
	return slugs, nil
}

// ------------------------ Step 2: from slugs → posts + user IDs ------------------------

func fetchUsersFromSlugs(slugs []string, postsRoot string) ([]string, error) {
	userSet := make(map[string]struct{})
	var userMu sync.Mutex

	jobs := make(chan string, *workers*2)
	var wg sync.WaitGroup

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for slug := range jobs {
				postURL := fmt.Sprintf("%s/%s.json", strings.TrimRight(*basePost, "/"), url.PathEscape(slug))

				postData, err := fetchJSONMap(postURL)
				if err != nil {
					log.Printf("[seed worker %d] post slug %s: %v\n", workerID, slug, err)
					continue
				}

				// Rewrite URLs
				postData = rewriteURLs(postData).(map[string]interface{})

				// Extract userId
				userID := ""
				if v, ok := postData["userIdStr"].(string); ok && v != "" {
					userID = v
				} else if f, ok := postData["userId"].(float64); ok {
					userID = fmt.Sprintf("%.0f", f)
				}

				// Extract real post ID
				realID := ""
				if v, ok := postData["postIdStr"].(string); ok && v != "" {
					realID = v
				} else if f, ok := postData["postId"].(float64); ok {
					realID = fmt.Sprintf("%.0f", f)
				} else {
					realID = slug
				}

				if userID == "" {
					continue
				}

				// Record userID
				userMu.Lock()
				if _, exists := userSet[userID]; !exists {
					userSet[userID] = struct{}{}
				}
				userMu.Unlock()

				// Save this post immediately under user
				userPostsDir := filepath.Join(postsRoot, userID)
				if err := os.MkdirAll(userPostsDir, 0755); err != nil {
					log.Printf("[seed worker %d] MkdirAll posts dir for %s: %v\n", workerID, userID, err)
					continue
				}
				postFile := filepath.Join(userPostsDir, realID+".json")
				if !fileExists(postFile) {
					if err := writeJSONFile(postFile, postData); err != nil {
						log.Printf("[seed worker %d] write seed post %s for user %s: %v\n",
							workerID, realID, userID, err)
					}
				}
			}
		}(i)
	}

	for _, slug := range slugs {
		jobs <- slug
	}
	close(jobs)
	wg.Wait()

	userIDs := make([]string, 0, len(userSet))
	for uid := range userSet {
		userIDs = append(userIDs, uid)
	}
	return userIDs, nil
}

// ------------------------ Step 3: per-user profile + posts ------------------------

func processUser(userID, profilesDir, postsRoot, mediaRoot string, workerID int) error {
	// 1) Ensure profile JSON exists
	profilePath := filepath.Join(profilesDir, userID+".json")
	if !fileExists(profilePath) {
		profileURL := fmt.Sprintf("%s/%s.json", strings.TrimRight(*baseProfile, "/"), url.PathEscape(userID))
		profile, err := fetchJSONMap(profileURL)
		if err != nil {
			return fmt.Errorf("fetch profile: %w", err)
		}
		// rewrite if you care about media URLs in profile
		profile = rewriteURLs(profile).(map[string]interface{})

		if err := writeJSONFile(profilePath, profile); err != nil {
			return fmt.Errorf("write profile JSON: %w", err)
		}
	}

	// 2) Load profile to get post IDs
	raw, err := os.ReadFile(profilePath)
	if err != nil {
		return fmt.Errorf("read profile JSON: %w", err)
	}
	var profile map[string]interface{}
	if err := json.Unmarshal(raw, &profile); err != nil {
		return fmt.Errorf("decode profile JSON: %w", err)
	}

	postIDs := collectPostIDsFromProfile(profile)
	if len(postIDs) == 0 {
		log.Printf("[worker %d] user %s: no post IDs in profile\n", workerID, userID)
		return nil
	}

	userPostsDir := filepath.Join(postsRoot, userID)
	if err := os.MkdirAll(userPostsDir, 0755); err != nil {
		return fmt.Errorf("MkdirAll userPostsDir: %w", err)
	}

	for _, pid := range postIDs {
		postURL := fmt.Sprintf("%s/%s.json", strings.TrimRight(*basePost, "/"), url.PathEscape(pid))

		postData, err := fetchJSONMap(postURL)
		if err != nil {
			log.Printf("[worker %d] user %s post %s: %v\n", workerID, userID, pid, err)
			continue
		}

		// Extract real post ID
		realID := ""
		if v, ok := postData["postIdStr"].(string); ok && v != "" {
			realID = v
		} else if f, ok := postData["postId"].(float64); ok {
			realID = fmt.Sprintf("%.0f", f)
		} else {
			realID = pid
		}

		postFile := filepath.Join(userPostsDir, realID+".json")
		if fileExists(postFile) {
			continue
		}

		postData = rewriteURLs(postData).(map[string]interface{})

		if err := writeJSONFile(postFile, postData); err != nil {
			log.Printf("[worker %d] user %s post %s write: %v\n", workerID, userID, realID, err)
		}

		if *download {
			mediaURLs := collectMediaURLs(postData)
			for _, mu := range mediaURLs {
				if err := downloadMedia(mu, mediaRoot); err != nil {
					log.Printf("[worker %d] user %s post %s media %s: %v\n",
						workerID, userID, realID, mu, err)
				}
			}
		}
	}

	return nil
}

// ------------------------ HTTP + JSON helpers ------------------------

func fetchJSONMap(u string) (map[string]interface{}, error) {
	<-rateLimiter // global throttle

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "VineFullHarvester/1.0")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", "https://archive.vine.co/")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("HTTP 403 Forbidden for %s", u)
		}
		return nil, fmt.Errorf("HTTP %d for %s", resp.StatusCode, u)
	}

	var m map[string]interface{}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func writeJSONFile(path string, data interface{}) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	// compact JSON: faster and smaller
	if err := enc.Encode(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ------------------------ URL rewriting ------------------------

func rewriteURLs(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, vv := range t {
			t[k] = rewriteURLs(vv)
		}
		return t
	case []interface{}:
		for i, vv := range t {
			t[i] = rewriteURLs(vv)
		}
		return t
	case string:
		s := t
		if strings.Contains(s, ".cdn.vine.co") {
			s = strings.ReplaceAll(s, "http://v.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "https://v.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "http://mtc.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "https://mtc.cdn.vine.co", "https://vines.s3.amazonaws.com")
		}
		return s
	default:
		return v
	}
}

// ------------------------ postId collection ------------------------

func collectPostIDsFromProfile(profile map[string]interface{}) []string {
	seen := make(map[string]struct{})
	var out []string

	addID := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	// 1) Preferred: profile["posts"] list
	if rawPosts, ok := profile["posts"]; ok && rawPosts != nil {
		switch v := rawPosts.(type) {
		case []interface{}:
			for _, item := range v {
				switch t := item.(type) {
				case string:
					addID(t)
				case float64:
					addID(fmt.Sprintf("%.0f", t))
				case map[string]interface{}:
					if s, ok2 := t["postIdStr"].(string); ok2 && s != "" {
						addID(s)
					} else if f, ok2 := t["postId"].(float64); ok2 {
						addID(fmt.Sprintf("%.0f", f))
					}
				default:
					// ignore
				}
			}
		}
	}

	// 2) Fallback: deep scan for postId/postIdStr anywhere
	if len(out) == 0 {
		var walk func(v interface{})
		walk = func(v interface{}) {
			switch t := v.(type) {
			case map[string]interface{}:
				for k, vv := range t {
					kl := strings.ToLower(k)
					if (kl == "postid" || kl == "postidstr") && vv != nil {
						switch idv := vv.(type) {
						case string:
							addID(idv)
						case float64:
							addID(fmt.Sprintf("%.0f", idv))
						}
					}
					walk(vv)
				}
			case []interface{}:
				for _, vv := range t {
					walk(vv)
				}
			default:
				// nothing
			}
		}
		walk(profile)
	}

	return out
}

// ------------------------ media URL collection + download ------------------------

func collectMediaURLs(root interface{}) []string {
	var urls []string
	var walk func(v interface{})
	walk = func(v interface{}) {
		switch t := v.(type) {
		case map[string]interface{}:
			for _, vv := range t {
				walk(vv)
			}
		case []interface{}:
			for _, vv := range t {
				walk(vv)
			}
		case string:
			s := t
			if strings.Contains(s, "vines.s3.amazonaws.com") {
				if strings.Contains(s, ".mp4") || strings.Contains(s, ".jpg") ||
					strings.Contains(s, ".jpeg") || strings.Contains(s, ".png") ||
					strings.Contains(s, ".gif") {
					urls = append(urls, s)
				}
			}
		}
	}
	walk(root)
	return urls
}

func downloadMedia(rawURL, mediaRoot string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return err
	}

	downloadedMedia.mu.Lock()
	if _, ok := downloadedMedia.m[rawURL]; ok {
		downloadedMedia.mu.Unlock()
		return nil
	}
	downloadedMedia.m[rawURL] = struct{}{}
	downloadedMedia.mu.Unlock()

	cleanPath := strings.TrimLeft(parsed.Path, "/")
	localPath := filepath.Join(mediaRoot, cleanPath)

	if fileExists(localPath) {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return err
	}

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "VineFullHarvesterMedia/1.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("media HTTP %d", resp.StatusCode)
	}

	tmp := localPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, localPath)
}
