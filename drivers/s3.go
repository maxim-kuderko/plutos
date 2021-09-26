package drivers

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
)

type S3 struct {
	lastFlushed time.Time
	sess        *session.Session
	uploader    *s3manager.Uploader
	w           io.WriteCloser
	wg          sync.WaitGroup

	cfg *S3Config
}

type S3Config struct {
	region, dataPrefix, bucket, sqsQueue string
	enableCompression                    bool
}

/*var (
	region     = os.Getenv(`S3_REGION`)
	dataPrefix = os.Getenv(`S3_PREFIX`)
	bucket     = os.Getenv(`S3_BUCKET`)
	sqsQueue   = os.Getenv(`SQS_QUEUE`)
)*/

func NewS3(cfg *S3Config) Driver {
	validateInitialSettings(cfg)
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(cfg.region),
	}))
	s := &S3{
		sess:        svc,
		uploader:    s3manager.NewUploader(svc),
		lastFlushed: time.Now(),
		cfg:         cfg,
	}
	var err error
	s.w, err = s.newUploader()
	if err != nil {
		panic(err)
	}
	return s
}

func validateInitialSettings(cfg *S3Config) {
	if cfg.region == `` {
		panic(`S3_REGION is empty`)
	}

	if cfg.bucket == `` {
		panic(`S3_BUCKET is empty`)
	}

	if cfg.dataPrefix == `` {
		panic(`S3_PREFIX is empty`)
	}

	if cfg.sqsQueue == `` {
		panic(`SQS_QUEUE is empty`)
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
	suffix := ``
	if so.cfg.enableCompression {
		suffix = `.gz`
	}
	key := fmt.Sprintf(`/%s/created_date=%s/hour=%s/%s%s`, so.cfg.dataPrefix, t.Format(`2006-01-02`), t.Format(`15`), uuid.New().String(), suffix)
	_, err := so.uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(so.cfg.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		logrus.Error(err)
		return
	}
	b, _ := jsoniter.ConfigFastest.Marshal(S3SqsEvent{
		WrittenAt: time.Now().Format(time.RFC3339),
		Records: []Record{
			{
				AwsRegion: so.cfg.region,
				S3: S3S{
					Bucket: Bucket{
						Name: so.cfg.bucket,
					},
					Object: Object{
						Key: key,
					},
				},
			},
		}})
	uid := uuid.New().String()
	if _, err := sqs.New(so.sess).SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(string(b)),
		MessageDeduplicationId: aws.String(uid),
		MessageGroupId:         aws.String(uid),
		QueueUrl:               aws.String(so.cfg.sqsQueue),
	}); err != nil {
		logrus.Error(err)
	}
}

type S3SqsEvent struct {
	Records   []Record `json:"Records"`
	WrittenAt string   `json:"written_at"`
}
type Record struct {
	AwsRegion string `json:"awsRegion"`
	S3        S3S    `json:"s3"`
}
type S3S struct {
	Bucket Bucket `json:"bucket"`
	Object Object `json:"object"`
}

type Bucket struct {
	Name string `json:"name"`
}
type Object struct {
	Key string `json:"key"`
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
