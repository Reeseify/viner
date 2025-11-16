// vine_full_harvest.go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
	flagInputDir  = flag.String("inputDir", "", "Input directory (local path or s3://bucket/prefix)")
	flagOutDir    = flag.String("outDir", "", "Output directory (local path or s3://bucket/prefix)")
	flagWorkers   = flag.Int("workers", 32, "Number of concurrent workers for reading input objects")
	flagDownload  = flag.Bool("download", false, "Currently unused; reserved for future MP4 downloading")
	flagLoopEvery = flag.Duration("loopEvery", 0, "If > 0, loop the harvest every given duration (e.g. 10m)")
)

// Simple helper to read env with a default.
func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// S3 client initializer that targets Cloudflare R2 (or any S3-compatible endpoint)
// via the S3_ENDPOINT environment variable.
func newS3Client() *s3.Client {
	region := getenvDefault("AWS_REGION", "auto")

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	endpoint := os.Getenv("S3_ENDPOINT") // e.g. https://<ACCOUNT_ID>.r2.cloudflarestorage.com

	if accessKey == "" || secretKey == "" || endpoint == "" {
		log.Fatalf("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_ENDPOINT must be set for S3/R2 access")
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					if service == s3.ServiceID {
						return aws.Endpoint{
							URL:               endpoint,
							HostnameImmutable: true,
						}, nil
					}
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				},
			),
		),
	)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	return s3.NewFromConfig(cfg)
}

// Represents either an S3 path or a local path.
type s3Path struct {
	Bucket string
	Prefix string
	Local  string
	S3     bool
}

func parsePath(p string) s3Path {
	if strings.HasPrefix(p, "s3://") {
		rest := strings.TrimPrefix(p, "s3://")
		parts := strings.SplitN(rest, "/", 2)
		bucket := parts[0]
		prefix := ""
		if len(parts) == 2 {
			prefix = parts[1]
		}
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		return s3Path{
			Bucket: bucket,
			Prefix: prefix,
			S3:     true,
		}
	}
	return s3Path{
		Local: p,
		S3:    false,
	}
}

// Finds all *.txt objects in an S3 bucket/prefix.
func listTxtObjects(ctx context.Context, client *s3.Client, sp s3Path) ([]types.Object, error) {
	log.Printf("Listing objects in bucket=%s prefix=%s", sp.Bucket, sp.Prefix)

	var txtObjects []types.Object
	var token *string

	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(sp.Bucket),
			Prefix:            aws.String(sp.Prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("listing input objects: %w", err)
		}

		for _, obj := range out.Contents {
			if strings.HasSuffix(strings.ToLower(*obj.Key), ".txt") {
				txtObjects = append(txtObjects, obj)
			}
		}

		if out.IsTruncated && out.NextContinuationToken != nil {
			token = out.NextContinuationToken
		} else {
			break
		}
	}

	return txtObjects, nil
}

// Extracts Vine slugs from a text blob by regex searching for vine.co/v/SLUG.
var vineSlugRe = regexp.MustCompile(`vine\.co/v/([A-Za-z0-9]+)`)

func extractSlugsFromReader(r io.Reader, slugs map[string]struct{}, mu *sync.Mutex) error {
	scanner := bufio.NewScanner(r)
	// Increase buffer in case some lines are huge.
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		matches := vineSlugRe.FindAllStringSubmatch(line, -1)
		if len(matches) == 0 {
			continue
		}
		mu.Lock()
		for _, m := range matches {
			if len(m) >= 2 {
				slug := m[1]
				slugs[slug] = struct{}{}
			}
		}
		mu.Unlock()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning input: %w", err)
	}
	return nil
}

// Read a single S3 object and extract Vine slugs.
func processS3Object(ctx context.Context, client *s3.Client, bucket, key string, slugs map[string]struct{}, mu *sync.Mutex) error {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("GetObject %s: %w", key, err)
	}
	defer resp.Body.Close()

	return extractSlugsFromReader(resp.Body, slugs, mu)
}

// For local inputDir: walk *.txt files.
func listLocalTxtFiles(sp s3Path) ([]string, error) {
	var files []string
	err := filepath.Walk(sp.Local, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && strings.HasSuffix(strings.ToLower(info.Name()), ".txt") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func processLocalFile(path string, slugs map[string]struct{}, mu *sync.Mutex) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	return extractSlugsFromReader(f, slugs, mu)
}

// Writes the collected slugs into outDir as vine_slugs.txt (S3 or local).
func writeSlugs(ctx context.Context, out s3Path, client *s3.Client, slugs map[string]struct{}) error {
	// Turn map into sorted slice (optional; unsorted is fine too).
	var list []string
	for slug := range slugs {
		list = append(list, slug)
	}
	// Not strictly required, but nicer / deterministic.
	// sort.Strings(list)

	var builder strings.Builder
	for _, slug := range list {
		builder.WriteString(slug)
		builder.WriteByte('\n')
	}
	data := []byte(builder.String())

	if out.S3 {
		key := out.Prefix + "vine_slugs.txt"
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(out.Bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(string(data)),
		})
		if err != nil {
			return fmt.Errorf("PutObject %s: %w", key, err)
		}
		log.Printf("Wrote slugs to s3://%s/%s", out.Bucket, key)
		return nil
	}

	// Local path
	if err := os.MkdirAll(out.Local, 0o755); err != nil {
		return fmt.Errorf("creating outDir %s: %w", out.Local, err)
	}
	dest := filepath.Join(out.Local, "vine_slugs.txt")
	if err := os.WriteFile(dest, data, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", dest, err)
	}
	log.Printf("Wrote slugs to %s", dest)
	return nil
}

func runOnce(ctx context.Context) error {
	if *flagInputDir == "" || *flagOutDir == "" {
		return fmt.Errorf("inputDir and outDir are required")
	}

	inPath := parsePath(*flagInputDir)
	outPath := parsePath(*flagOutDir)

	s3Client := (*s3.Client)(nil)
	if inPath.S3 || outPath.S3 {
		s3Client = newS3Client()
	}

	slugs := make(map[string]struct{})
	var mu sync.Mutex

	if inPath.S3 {
		log.Printf("=== Scanning %s for Vine video URLs (S3/R2) ===", *flagInputDir)
		objs, err := listTxtObjects(ctx, s3Client, inPath)
		if err != nil {
			return err
		}
		log.Printf("Found %d .txt objects in S3/R2", len(objs))

		// Worker pool for parallel object processing
		type job struct {
			Key string
		}

		jobs := make(chan job, *flagWorkers)
		var wg sync.WaitGroup

		workerCount := *flagWorkers
		if workerCount < 1 {
			workerCount = 1
		}

		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range jobs {
					if err := processS3Object(ctx, s3Client, inPath.Bucket, j.Key, slugs, &mu); err != nil {
						log.Printf("error processing %s: %v", j.Key, err)
					}
				}
			}()
		}

		for _, obj := range objs {
			if obj.Key == nil {
				continue
			}
			jobs <- job{Key: *obj.Key}
		}
		close(jobs)
		wg.Wait()

	} else {
		log.Printf("=== Scanning %s for Vine video URLs (local) ===", inPath.Local)
		files, err := listLocalTxtFiles(inPath)
		if err != nil {
			return fmt.Errorf("listing local txt files: %w", err)
		}
		log.Printf("Found %d .txt files locally", len(files))

		for _, path := range files {
			if err := processLocalFile(path, slugs, &mu); err != nil {
				log.Printf("error processing %s: %v", path, err)
			}
		}
	}

	log.Printf("Collected %d unique Vine slugs", len(slugs))

	if err := writeSlugs(ctx, outPath, s3Client, slugs); err != nil {
		return err
	}

	log.Printf("Scan complete.")
	return nil
}

func main() {
	flag.Parse()

	if *flagInputDir == "" || *flagOutDir == "" {
		log.Fatalf("inputDir and outDir are required")
	}

	ctx := context.Background()

	if *flagLoopEvery <= 0 {
		if err := runOnce(ctx); err != nil {
			log.Fatalf("runOnce failed: %v", err)
		}
		return
	}

	// Looping mode for continuous updates.
	for {
		if err := runOnce(ctx); err != nil {
			log.Printf("runOnce failed: %v", err)
		}
		log.Printf("Sleeping for %s before next run...", *flagLoopEvery)
		time.Sleep(*flagLoopEvery)
	}
}
