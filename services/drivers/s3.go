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
	region       = os.Getenv(`S3_REGION`)
	dataPrefix   = os.Getenv(`S3_PREFIX`)
	bucket       = os.Getenv(`S3_BUCKET`)
	isCompressed = os.Getenv(`S3_IS_COMPRESSED`) == `true`
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
	if isCompressed {
		return newGzipper(w)
	}
	return w, nil
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

// not go routine safe
func (so *S3) Write(e []byte) (int, error) {
	return so.w.Write(e)
}

// not go routine safe
func (so *S3) Flush() {
	tmp := so.w
	go tmp.Close()
	var err error
	so.w, err = so.newUploader()
	log.Err(err)
	return
}

func (so *S3) Close() error {
	defer fmt.Println(`closed`)
	if err := so.w.Close(); err != nil {
		return err
	}
	so.wg.Wait()
	return nil
}
