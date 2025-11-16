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
)

var (
	inputDir  = flag.String("inputDir", "", "Input dir: local path or s3://bucket/prefix")
	outDir    = flag.String("outDir", "", "Output dir: local path or s3://bucket/prefix")
	workers   = flag.Int("workers", 32, "Number of concurrent workers")
	download  = flag.Bool("download", false, "Unused for now (kept for compatibility)")
	loopEvery = flag.Duration("loopEvery", 0, "If >0, rescan every given interval (e.g. 10m, 1h)")
)

// regex to find vine.co/v/SLUG
var vineRe = regexp.MustCompile(`vine\.co/v/([A-Za-z0-9]+)`)

type sourceType int

const (
	srcLocal sourceType = iota
	srcS3
)

type s3Path struct {
	Bucket string
	Prefix string
}

func getenvDefault(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}


func main() {
	flag.Parse()

	if *inputDir == "" || *outDir == "" {
		log.Fatal("inputDir and outDir are required")
	}

	srcKind := detectSource(*inputDir)

	ctx := context.Background()

	var s3Client *s3.Client
	var inS3, outS3 *s3Path

	if srcKind == srcS3 || strings.HasPrefix(*outDir, "s3://") {
		var err error
		s3Client, err = newS3Client(ctx)
		if err != nil {
			log.Fatalf("failed to create S3 client: %v", err)
		}
	}

	if srcKind == srcS3 {
		p, err := parseS3Path(*inputDir)
		if err != nil {
			log.Fatalf("invalid inputDir %q: %v", *inputDir, err)
		}
		inS3 = p
	}

	if strings.HasPrefix(*outDir, "s3://") {
		p, err := parseS3Path(*outDir)
		if err != nil {
			log.Fatalf("invalid outDir %q: %v", *outDir, err)
		}
		outS3 = p
	}

	for {
		if err := runOnce(ctx, srcKind, s3Client, inS3, outS3); err != nil {
			log.Fatalf("runOnce failed: %v", err)
		}

		if *loopEvery == 0 {
			// one-shot run (current behavior)
			return
		}

		log.Printf("Scan complete. Sleeping for %s before next run...", *loopEvery)
		time.Sleep(*loopEvery)
	}
}

func runOnce(ctx context.Context, srcKind sourceType, s3Client *s3.Client, inS3, outS3 *s3Path) error {
	log.Printf("=== Scanning %s for Vine video URLs (S3/R2) ===", *inputDir)

	var keys []string
	var err error

	switch srcKind {
	case srcS3:
		if inS3 == nil {
			return fmt.Errorf("internal error: inS3 nil")
		}
		log.Printf("Listing objects in bucket=%s prefix=%s", inS3.Bucket, inS3.Prefix)
		keys, err = listS3TextObjects(ctx, s3Client, inS3.Bucket, inS3.Prefix)
	default:
		log.Printf("Listing local .txt files under %s", *inputDir)
		keys, err = listLocalTextFiles(*inputDir)
	}
	if err != nil {
		return fmt.Errorf("listing input objects: %w", err)
	}

	log.Printf("Found %d .txt objects in %s", len(keys), sourceName(srcKind))

	slugs := make(map[string]struct{})
	var mu sync.Mutex

	jobs := make(chan string)
	var wg sync.WaitGroup

	workerCount := *workers
	if workerCount < 1 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				var content io.ReadCloser
				var err error

				switch srcKind {
				case srcS3:
					content, err = readS3Object(ctx, s3Client, inS3.Bucket, key)
				default:
					content, err = readLocalFile(key)
				}
				if err != nil {
					log.Printf("error reading %s: %v", key, err)
					continue
				}

				// Always close
				func() {
					defer content.Close()
					fileSlugs := extractVineSlugs(content)
					if len(fileSlugs) == 0 {
						return
					}
					mu.Lock()
					for _, s := range fileSlugs {
						slugs[s] = struct{}{}
					}
					mu.Unlock()
				}()
			}
		}()
	}

	for _, k := range keys {
		jobs <- k
	}
	close(jobs)
	wg.Wait()

	log.Printf("Collected %d unique Vine slugs", len(slugs))

	if outS3 != nil {
		return writeSlugsToS3(ctx, s3Client, outS3.Bucket, outS3.Prefix, slugs)
	}
	return writeSlugsToLocal(*outDir, slugs)
}

// --- helpers ---

func detectSource(path string) sourceType {
	if strings.HasPrefix(path, "s3://") {
		return srcS3
	}
	return srcLocal
}

func sourceName(s sourceType) string {
	if s == srcS3 {
		return "S3/R2"
	}
	return "local filesystem"
}

func parseS3Path(p string) (*s3Path, error) {
	if !strings.HasPrefix(p, "s3://") {
		return nil, fmt.Errorf("must start with s3://")
	}
	trimmed := strings.TrimPrefix(p, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("missing bucket in %q", p)
	}
	path := &s3Path{Bucket: parts[0]}
	if len(parts) == 2 {
		path.Prefix = strings.TrimPrefix(parts[1], "/")
	}
	return path, nil
}

func newS3Client() *s3.Client {
    region := getenvDefault("AWS_REGION", "auto")

    accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
    secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
    endpoint := os.Getenv("S3_ENDPOINT") // <-- will point to Cloudflare R2

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

	s3Client := newS3Client()
    return s3Client
}


func listS3TextObjects(ctx context.Context, client *s3.Client, bucket, prefix string) ([]string, error) {
	var keys []string
	var token *string

	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			if strings.HasSuffix(*obj.Key, ".txt") {
				keys = append(keys, *obj.Key)
			}
		}

		if out.IsTruncated && out.NextContinuationToken != nil {
			token = out.NextContinuationToken
		} else {
			break
		}
	}
	return keys, nil
}

func readS3Object(ctx context.Context, client *s3.Client, bucket, key string) (io.ReadCloser, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func listLocalTextFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".txt") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func readLocalFile(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func extractVineSlugs(r io.Reader) []string {
	var slugs []string
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024) // up to 10MB per line

	for scanner.Scan() {
		line := scanner.Text()
		matches := vineRe.FindAllStringSubmatch(line, -1)
		for _, m := range matches {
			if len(m) >= 2 {
				slugs = append(slugs, m[1])
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("scanner error: %v", err)
	}
	return slugs
}

func writeSlugsToLocal(outRoot string, slugs map[string]struct{}) error {
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return fmt.Errorf("creating outDir: %w", err)
	}
	outPath := filepath.Join(outRoot, "vine_slugs.txt")
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for s := range slugs {
		if _, err := w.WriteString(s + "\n"); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	log.Printf("Wrote %d slugs to %s", len(slugs), outPath)
	return nil
}

func writeSlugsToS3(ctx context.Context, client *s3.Client, bucket, prefix string, slugs map[string]struct{}) error {
	var sb strings.Builder
	for s := range slugs {
		sb.WriteString(s)
		sb.WriteString("\n")
	}

	key := strings.TrimPrefix(filepath.Join(prefix, "vine_slugs.txt"), "/")

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(sb.String()),
	})
	if err != nil {
		return fmt.Errorf("put object to s3://%s/%s: %w", bucket, key, err)
	}
	log.Printf("Wrote %d slugs to s3://%s/%s", len(slugs), bucket, key)
	return nil
}
