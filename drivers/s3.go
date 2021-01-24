package drivers

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"sync"
	"time"
)

type S3 struct {
	lastFlushed time.Time
	uploader    *s3manager.Uploader
	w           io.WriteCloser
	wg          sync.WaitGroup
}

var (
	region     = os.Getenv(`S3_REGION`)
	dataPrefix = os.Getenv(`S3_PREFIX`)
	bucket     = os.Getenv(`S3_BUCKET`)
)

func NewS3() Driver {
	validateInitialSettings()

	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	s := &S3{
		uploader:    s3manager.NewUploader(svc),
		lastFlushed: time.Now(),
	}
	var err error
	s.w, err = s.newUploader()
	if err != nil {
		panic(err)
	}
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
}

func (so *S3) newUploader() (io.WriteCloser, error) {
	r, w := io.Pipe()
	so.wg.Add(1)
	go so.upload(r)
	return w, nil
}

func (so *S3) upload(r *io.PipeReader) {
	defer so.wg.Done()
	t := time.Now()
	_, err := so.uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(fmt.Sprintf(`/%s/created_date=%s/hour=%s/%s`, dataPrefix, t.Format(`2006-01-02`), t.Format(`15`), uuid.New().String())),
	})
	if err != nil {
		log.Error().Stack().Err(err).Msg("")
	}

}

// not go routine safe
func (so *S3) Write(e []byte) (int, error) {
	return so.w.Write(e)
}

func (so *S3) Close() error {
	if err := so.w.Close(); err != nil {
		return err
	}
	so.wg.Wait()
	return nil
}
