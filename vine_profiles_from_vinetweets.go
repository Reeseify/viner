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
    "strings"
    "sync"
    "time"
)

// Minimal post structure: we only care about userIdStr.
type Post struct {
	UserIdStr string `json:"userIdStr"`
}

// Flags
var (
	inputDir        = flag.String("inputDir", "E:/vine_tweets", "Directory containing Vine-Tweets *.txt files")
	postBase        = flag.String("postBase", "https://archive.vine.co/posts", "Base URL for post JSON (no trailing slash)")
	outProfilesJSON = flag.String("outProfilesJson", "profiles.json", "Output JSON file for userIdStr list")
	workers         = flag.Int("workers", 64, "Number of concurrent HTTP workers")
	limit           = flag.Int("limit", 0, "Optional limit on number of video IDs to process (0 = all)")
)

func main() {
	flag.Parse()

	// 1) Collect unique video IDs from Vine-Tweets text files
	ids, err := collectVideoIDs(*inputDir)
	if err != nil {
		log.Fatalf("collectVideoIDs failed: %v", err)
	}
	if len(ids) == 0 {
		log.Println("No video IDs found. Check inputDir and that you extracted the dataset .txt files.")
		return
	}

	if *limit > 0 && len(ids) > *limit {
		log.Printf("Limiting to first %d IDs (of %d)\n", *limit, len(ids))
		ids = ids[:*limit]
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	// Collect unique userIdStr values
	userIDs := make(map[string]struct{})
	var mu sync.Mutex

	type job struct {
		id string
	}
	jobs := make(chan job, *workers*2)

	// 2) Worker pool to fetch post JSONs
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range jobs {
				p, err := fetchPost(client, *postBase, j.id)
				if err != nil {
					// 404s are expected for some IDs – only log noisy stuff occasionally.
					if !strings.Contains(err.Error(), "status 404") {
						log.Printf("[worker %d] %s: %v\n", workerID, j.id, err)
					}
					continue
				}
				if p.UserIdStr == "" {
					continue
				}

				mu.Lock()
				userIDs[p.UserIdStr] = struct{}{}
				mu.Unlock()
			}
		}(i)
	}

	start := time.Now()

	// Feed jobs
	go func() {
		for _, id := range ids {
			jobs <- job{id: id}
		}
		close(jobs)
	}()

	wg.Wait()
	log.Printf("Finished fetching posts in %v\n", time.Since(start))

	// 3) Write profiles.json as array of userIdStr strings
	if err := writeUserIDsJSON(*outProfilesJSON, userIDs); err != nil {
		log.Fatalf("writeUserIDsJSON failed: %v", err)
	}
	log.Printf("Done. Unique userIdStr count: %d\n", len(userIDs))
}

// ----------------- Collect video IDs from Vine-Tweets dataset -----------------

func collectVideoIDs(dir string) ([]string, error) {
	log.Println("Scanning for Vine-Tweets text files in:", dir)

	idsSet := make(map[string]struct{})

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			log.Printf("Skipping %s: %v\n", path, err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".txt") {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			log.Printf("Failed to open %s: %v\n", path, err)
			return nil
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			rawURL := fields[1]
			id := extractVineID(rawURL)
			if id == "" {
				continue
			}
			idsSet[id] = struct{}{}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error scanning %s: %v\n", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(idsSet))
	for id := range idsSet {
		ids = append(ids, id)
	}
	log.Printf("Collected %d unique vine video IDs\n", len(ids))
	return ids, nil
}

func extractVineID(u string) string {
	// examples:
	// https://vine.co/v/5AizwaPT2EO
	// http://vine.co/v/5AizwaPT2EO/
	// https://vine.co/v/5AizwaPT2EO?something=1
	if !strings.Contains(u, "vine.co/v/") {
		return ""
	}
	// Strip protocol
	if strings.HasPrefix(u, "http://") {
		u = u[len("http://"):]
	} else if strings.HasPrefix(u, "https://") {
		u = u[len("https://"):]
	}

	idx := strings.Index(u, "/v/")
	if idx == -1 {
		return ""
	}
	idPart := u[idx+3:] // after "/v/"
	// strip query + trailing slash
	if qIdx := strings.Index(idPart, "?"); qIdx != -1 {
		idPart = idPart[:qIdx]
	}
	idPart = strings.Trim(idPart, "/ \t")
	return idPart
}

// ----------------- HTTP fetching -----------------

func fetchPost(client *http.Client, base, id string) (*Post, error) {
	u := fmt.Sprintf("%s/%s.json", strings.TrimRight(base, "/"), url.PathEscape(id))

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "VineArchiveProfileHarvester/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Many IDs will 404; that’s fine.
	if resp.StatusCode != http.StatusOK {
		// Drain body and return error
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var p Post
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// ----------------- Output -----------------

func writeUserIDsJSON(path string, ids map[string]struct{}) error {
	log.Println("Writing userIdStr list to", path)
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	slice := make([]string, 0, len(ids))
	for id := range ids {
		slice = append(slice, id)
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(slice)
}
