package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Regex to find Vine video URLs and capture the slug.
var vineURLRe = regexp.MustCompile(`https?://vine\.co/v/([A-Za-z0-9]+)`)

func main() {
	inputDir := flag.String("inputDir", "", "Local dir or s3://bucket/prefix/")
	outDir := flag.String("outDir", "vine_archive_harvest", "Output directory")
	workers := flag.Int("workers", runtime.NumCPU()*4, "Number of worker goroutines")
	download := flag.Bool("download", false, "Download videos (not implemented; reserved)")
	flag.Parse()

	if *inputDir == "" {
		log.Fatal("-inputDir is required")
	}

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("create outDir %s: %v", *outDir, err)
	}

	start := time.Now()

	var (
		slugs map[string]struct{}
		err   error
	)

	if strings.HasPrefix(*inputDir, "s3://") {
		// ----- S3 / R2 path -----
		ctx := context.Background()
		client, bucket, prefix, errInit := newS3ClientFromEnv(ctx, *inputDir)
		if errInit != nil {
			log.Fatalf("init S3 client: %v", errInit)
		}
		log.Printf("=== Scanning %s for Vine video URLs (S3/R2) ===", *inputDir)
		slugs, err = scanS3ForSlugs(ctx, client, bucket, prefix, *workers)
	} else {
		// ----- Local filesystem path -----
		log.Printf("=== Scanning %s for Vine video URLs (local) ===", *inputDir)
		slugs, err = scanLocalForSlugs(*inputDir, *workers)
	}

	if err != nil {
		log.Fatalf("scan failed: %v", err)
	}

	// Turn map into sorted slice.
	list := make([]string, 0, len(slugs))
	for slug := range slugs {
		list = append(list, slug)
	}
	sort.Strings(list)

	if err := writeOutputs(*outDir, list); err != nil {
		log.Fatalf("write outputs: %v", err)
	}

	log.Printf("Done. Found %d unique Vine slugs in %s", len(list), time.Since(start))
	if *download {
		log.Printf("Note: -download=true is not implemented in this stripped-down harvester.")
	}
}

// writeOutputs writes vine_slugs.txt and vine_slugs.json.
func writeOutputs(outDir string, slugs []string) error {
	txtPath := filepath.Join(outDir, "vine_slugs.txt")
	jsonPath := filepath.Join(outDir, "vine_slugs.json")

	// Text file
	tf, err := os.Create(txtPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", txtPath, err)
	}
	defer tf.Close()

	for _, s := range slugs {
		if _, err := fmt.Fprintln(tf, s); err != nil {
			return fmt.Errorf("write %s: %w", txtPath, err)
		}
	}

	// JSON file
	jf, err := os.Create(jsonPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", jsonPath, err)
	}
	defer jf.Close()

	enc := json.NewEncoder(jf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(slugs); err != nil {
		return fmt.Errorf("encode %s: %w", jsonPath, err)
	}

	log.Printf("Wrote %s and %s", txtPath, jsonPath)
	return nil
}

//
// ---------- Local filesystem scanning ----------
//

func scanLocalForSlugs(root string, workers int) (map[string]struct{}, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}

	paths := []string{}
	if err := filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".txt") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}

	log.Printf("Found %d local .txt files to scan", len(paths))

	return extractSlugsFromFiles(paths, workers, func(path string) (bufio.Scanner, func() error, error) {
		f, err := os.Open(path)
		if err != nil {
			return bufio.Scanner{}, nil, fmt.Errorf("open %s: %w", path, err)
		}
		sc := *bufio.NewScanner(f)
		cleanup := func() error { return f.Close() }
		return sc, cleanup, nil
	})
}

//
// ---------- S3 / R2 scanning ----------
//

func newS3ClientFromEnv(ctx context.Context, s3URL string) (*s3.Client, string, string, error) {
	u, err := url.Parse(s3URL)
	if err != nil {
		return nil, "", "", fmt.Errorf("parse inputDir %q: %w", s3URL, err)
	}
	if u.Scheme != "s3" {
		return nil, "", "", fmt.Errorf("inputDir must start with s3://, got %q", s3URL)
	}
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")

	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("R2_ENDPOINT")
	}
	if endpoint == "" {
		return nil, "", "", fmt.Errorf("S3_ENDPOINT or R2_ENDPOINT env var must be set for R2")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		// Cloudflare suggests "auto" but AWS SDK requires something non-empty.
		region = "auto"
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return nil, "", "", fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awscfg.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, r string, _ ...any) (aws.Endpoint, error) {
				// Use the same endpoint for all S3 requests.
				return aws.Endpoint{
					URL:           endpoint,
					PartitionID:   "aws",
					SigningRegion: region,
				}, nil
			}),
		),
	)
	if err != nil {
		return nil, "", "", fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// R2 usually wants path-style.
		o.UsePathStyle = true
	})

	return client, bucket, prefix, nil
}

func scanS3ForSlugs(ctx context.Context, client *s3.Client, bucket, prefix string, workers int) (map[string]struct{}, error) {
	log.Printf("Listing objects in bucket=%s prefix=%s", bucket, prefix)

	keys := []string{}

	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: aws.String(prefix),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".txt") {
				keys = append(keys, *obj.Key)
			}
		}
	}

	log.Printf("Found %d .txt objects in S3/R2", len(keys))

	return extractSlugsFromFiles(keys, workers, func(key string) (bufio.Scanner, func() error, error) {
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			return bufio.Scanner{}, nil, fmt.Errorf("get %s: %w", key, err)
		}
		sc := *bufio.NewScanner(out.Body)
		cleanup := func() error { return out.Body.Close() }
		return sc, cleanup, nil
	})
}

//
// ---------- Shared slug extraction logic ----------
//

// fileOpener abstracts over local files and S3 objects.
type fileOpener func(pathOrKey string) (bufio.Scanner, func() error, error)

func extractSlugsFromFiles(paths []string, workers int, open fileOpener) (map[string]struct{}, error) {
	slugs := make(map[string]struct{})
	var mu sync.Mutex

	jobs := make(chan string, workers*2)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	worker := func() {
		defer wg.Done()
		for p := range jobs {
			sc, cleanup, err := open(p)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			for sc.Scan() {
				line := sc.Bytes()
				matches := vineURLRe.FindAllSubmatch(line, -1)
				if len(matches) == 0 {
					continue
				}
				mu.Lock()
				for _, m := range matches {
					if len(m) > 1 {
						slugs[string(m[1])] = struct{}{}
					}
				}
				mu.Unlock()
			}
			if err := sc.Err(); err != nil {
				log.Printf("scan error in %s: %v", p, err)
			}
			if err := cleanup(); err != nil {
				log.Printf("cleanup error for %s: %v", p, err)
			}
		}
	}

	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		for _, p := range paths {
			jobs <- p
		}
		close(jobs)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		return nil, err
	case <-done:
	}

	return slugs, nil
}
