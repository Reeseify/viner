package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Flags
var (
	profilesPath = flag.String("profiles", "profiles.json", "JSON file containing userIdStr list (array of strings)")
	outDir       = flag.String("outDir", "vine_archive_harvest", "Output root directory")
	baseProfile  = flag.String("baseProfile", "https://archive.vine.co/profiles", "Base URL for profile JSON (no trailing slash)")
	basePost     = flag.String("basePost", "https://archive.vine.co/posts", "Base URL for post JSON (no trailing slash)")
	workers      = flag.Int("workers", 64, "Number of concurrent user workers")
	download     = flag.Bool("download", false, "Download media files from vines.s3.amazonaws.com")
)

// HTTP client (shared)
var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

// downloadedMedia keeps us from downloading the same file more than once.
var downloadedMedia = struct {
	mu sync.Mutex
	m  map[string]struct{}
}{m: make(map[string]struct{})}

var (
    // ~10 requests per second globally (tweak if you want)
    rateLimiter = time.Tick(time.Second / 10)
)


// ------------------------ main ------------------------

func main() {
	flag.Parse()

	userIDs, err := loadUserIDs(*profilesPath)
	if err != nil {
		log.Fatalf("loadUserIDs: %v", err)
	}
	if len(userIDs) == 0 {
		log.Fatalf("No user IDs found in %s", *profilesPath)
	}
	log.Printf("Loaded %d user IDs from %s\n", len(userIDs), *profilesPath)

	// Prepare directories
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

	// User job channel
	jobs := make(chan string, *workers*2)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for uid := range jobs {
				if err := processUser(uid, profilesDir, postsRoot, mediaRoot); err != nil {
					log.Printf("[worker %d] user %s: %v\n", workerID, uid, err)
				}
			}
		}(i)
	}

	start := time.Now()
	for _, uid := range userIDs {
		jobs <- uid
	}
	close(jobs)
	wg.Wait()

	log.Printf("Finished in %v\n", time.Since(start))
}

// ------------------------ load userId list ------------------------

func loadUserIDs(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// First try: array of strings: ["1206108953726337024", ...]
	var ids []string
	if err := json.Unmarshal(data, &ids); err == nil && len(ids) > 0 {
		return ids, nil
	}

	// Fallback: array of objects [{ "userId": "...", ... }]
	var objs []map[string]interface{}
	if err := json.Unmarshal(data, &objs); err == nil && len(objs) > 0 {
		for _, obj := range objs {
			if v, ok := obj["userIdStr"].(string); ok && v != "" {
				ids = append(ids, v)
			} else if v, ok2 := obj["userId"].(string); ok2 && v != "" {
				ids = append(ids, v)
			}
		}
	}
	return ids, nil
}

// ------------------------ per-user processing ------------------------

func processUser(userID, profilesDir, postsRoot, mediaRoot string) error {
	// 1) Fetch profile JSON
	profileURL := fmt.Sprintf("%s/%s.json", strings.TrimRight(*baseProfile, "/"), url.PathEscape(userID))
	profile, err := fetchJSONMap(profileURL)
	if err != nil {
		return fmt.Errorf("fetch profile: %w", err)
	}

	// Rewrite URLs inside profile
	profile = rewriteURLs(profile).(map[string]interface{})

	// Save profile JSON
	profilePath := filepath.Join(profilesDir, userID+".json")
	if err := writeJSONFile(profilePath, profile); err != nil {
		return fmt.Errorf("write profile JSON: %w", err)
	}

	// 2) Find all post IDs in profile JSON (generic scan for postId / postIdStr)
	postIDs := collectPostIDsFromProfile(profile)
	if len(postIDs) == 0 {
		// Some profiles may not expose posts; not necessarily an error.
		log.Printf("User %s: no post IDs found in profile\n", userID)
		return nil
	}

	userPostsDir := filepath.Join(postsRoot, userID)
	if err := os.MkdirAll(userPostsDir, 0755); err != nil {
		return fmt.Errorf("MkdirAll userPostsDir: %w", err)
	}

	for _, pid := range postIDs {
		postFile := filepath.Join(userPostsDir, pid+".json")
		if fileExists(postFile) {
			// Already harvested
			continue
		}
		postURL := fmt.Sprintf("%s/%s.json", strings.TrimRight(*basePost, "/"), url.PathEscape(pid))

		postData, err := fetchJSONMap(postURL)
		if err != nil {
			// Posts disappear or some IDs are bogus; log and continue.
			log.Printf("User %s post %s: %v\n", userID, pid, err)
			continue
		}

		// Rewrite URLs
		postData = rewriteURLs(postData).(map[string]interface{})

		// Save JSON
		if err := writeJSONFile(postFile, postData); err != nil {
			log.Printf("User %s post %s: write JSON: %v\n", userID, pid, err)
		}

		// Download media if toggled
		if *download {
			mediaURLs := collectMediaURLs(postData)
			for _, mu := range mediaURLs {
				if err := downloadMedia(mu, mediaRoot); err != nil {
					log.Printf("User %s post %s: download %s: %v\n", userID, pid, mu, err)
				}
			}
		}
	}

	return nil
}

// ------------------------ HTTP + JSON helpers ------------------------

func fetchJSONMap(u string) (map[string]interface{}, error) {
    <-rateLimiter  // global throttle

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "FastVineHarvester/1.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
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
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		f.Close()
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

// rewriteURLs walks any JSON structure and:
//   - replaces http(s)://v.cdn.vine.co/... with https://vines.s3.amazonaws.com/...
//   - leaves existing https://vines.s3.amazonaws.com/... as-is.
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
		if strings.Contains(s, "v.cdn.vine.co") {
			s = strings.ReplaceAll(s, "http://v.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "https://v.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "https://mtc.cdn.vine.co", "https://vines.s3.amazonaws.com")
			s = strings.ReplaceAll(s, "http://mtc.cdn.vine.co", "https://vines.s3.amazonaws.com")
		}
		return s
	default:
		return v
	}
}

// ------------------------ postId collection ------------------------

// collectPostIDsFromProfile pulls post IDs from a profile JSON.
// It first looks for a top-level "posts" field (which is usually just a list
// of IDs), and falls back to a deep scan for postId/postIdStr if needed.
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

	// 1) Preferred: profile["posts"] as a flat list of IDs
	if rawPosts, ok := profile["posts"]; ok && rawPosts != nil {
		switch v := rawPosts.(type) {
		case []interface{}:
			for _, item := range v {
				switch t := item.(type) {
				case string:
					addID(t)
				case float64:
					// numbers come in as float64 from JSON
					addID(fmt.Sprintf("%.0f", t))
				case map[string]interface{}:
					// just in case some variants store objects under posts
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

	// 2) Fallback: deep scan for postId / postIdStr anywhere
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

	// Only bother with fallback if we didn't get anything from "posts"
	if len(out) == 0 {
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
				// Basic filter: look for file-ish endings
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
	// dedupe
	downloadedMedia.mu.Lock()
	if _, ok := downloadedMedia.m[rawURL]; ok {
		downloadedMedia.mu.Unlock()
		return nil
	}
	downloadedMedia.m[rawURL] = struct{}{}
	downloadedMedia.mu.Unlock()

	// Build local path from URL path
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
	req.Header.Set("User-Agent", "FastVineHarvesterMedia/1.0")

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
