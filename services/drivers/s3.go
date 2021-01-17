package drivers

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type S3 struct {
	CurrentSize []int
	lastFlushed []time.Time
	uploader    *s3manager.Uploader
	w           []io.WriteCloser

	mu []sync.Mutex
	wg sync.WaitGroup
}

var (
	region                = os.Getenv(`S3_REGION`)
	dataPrefix            = os.Getenv(`S3_PREFIX`)
	bucket                = os.Getenv(`S3_BUCKET`)
	isCompressed          = os.Getenv(`S3_IS_COMPRESSED`) == `true`
	writingConnections, _ = strconv.Atoi(os.Getenv(`S3_WRITING_CONNECTIONS`))
	maxTime, _            = strconv.Atoi(os.Getenv(`S3_MAX_BUFFER_TIME_SECONDS`))
	maxFileSize, _        = strconv.Atoi(os.Getenv(`S3_MAX_FILE_SIZE`))
)

func NewS3() io.WriteCloser {
	validateInitialSettings()

	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	s := &S3{
		CurrentSize: make([]int, writingConnections),

		uploader: s3manager.NewUploader(svc),
		mu:       make([]sync.Mutex, writingConnections),
	}
	t := time.Now()
	lastFlushed := make([]time.Time, 0, writingConnections)
	writers := make([]io.WriteCloser, 0, writingConnections)
	for i := 0; i < writingConnections; i++ {
		lastFlushed = append(lastFlushed, t)
		w, err := s.newUploader()
		if err != nil {
			panic(err)
		}
		writers = append(writers, w)
	}

	s.lastFlushed = lastFlushed
	s.w = writers
	go s.periodicFlush()

	return s
}

func validateInitialSettings() {
	if region == `` {
		panic(`S3_REGION is empty`)
	}

	if bucket == `` {
		panic(`S3_BUCKET is empty`)
	}

	if dataPrefix == `` {
		panic(`S3_PREFIX is empty`)
	}

	if writingConnections == 0 {
		panic(`S3_WRITING_CONNECTIONS is empty`)
	}

	if maxTime == 0 {
		panic(`S3_MAX_BUFFER_TIME_SECONDS is empty`)
	}
	if maxFileSize == 0 {
		panic(`S3_MAX_FILE_SIZE is empty`)
	}
}

func (so *S3) newUploader() (io.WriteCloser, error) {
	r, w := io.Pipe()
	so.wg.Add(1)
	go so.upload(r)
	if isCompressed {
		return newGzipper(w)
	}
	return w, nil
}
func (so *S3) periodicFlush() {
	ticker := time.NewTicker(time.Second)
	maxTimeBetweenFlushes := time.Duration(maxTime) * time.Second
	for range ticker.C {
		for i := 0; i < writingConnections; i++ {
			so.mu[i].Lock()
			if time.Since(so.lastFlushed[i]) > maxTimeBetweenFlushes && so.CurrentSize[i] > 0 {
				if err := so.flush(i); err != nil {
					log.Err(err)
				}
			}
			so.mu[i].Unlock()

		}
	}

}

func (so *S3) upload(r *io.PipeReader) {
	defer so.wg.Done()
	t := time.Now()
	uid, err := uuid.NewUUID()
	if err != nil {
		log.Err(err)
		return
	}
	_, err = so.uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf(`/%s/created_date=%s/hour=%s/%s`, dataPrefix, t.Format(`2006-01-02`), t.Format(`15`), uid.String())),
	})
	if err != nil {
		log.Err(err)
	}

}

func (so *S3) Write(e []byte) (int, error) {
	r := rand.Int() % writingConnections
	so.mu[r].Lock()
	defer so.mu[r].Unlock()
	defer func() {
		so.CurrentSize[r] += len(e)
		if so.CurrentSize[r] > maxFileSize*1024*1024 {
			if err := so.flush(r); err != nil {
				log.Err(err)
			}
		}
	}()
	return so.w[r].Write(e)
}

// not go routine safe
func (so *S3) flush(r int) error {
	tmp := so.w[r]
	go tmp.Close()
	var err error
	so.w[r], err = so.newUploader()
	so.CurrentSize[r] = 0
	so.lastFlushed[r] = time.Now()
	return err
}

func (so *S3) Close() error {
	for i := 0; i < writingConnections; i++ {
		so.mu[i].Lock()
		if err := so.w[i].Close(); err != nil {
			return err
		}
	}
	so.wg.Wait()
	fmt.Println(`closed`)
	return nil
}
