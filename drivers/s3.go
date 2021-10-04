package drivers

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"io"
	"strings"
	"sync"
	"time"
)

type S3 struct {
	lastFlushed time.Time
	sess        *session.Session
	uploader    *s3manager.Uploader
	w           io.WriteCloser
	wg          sync.WaitGroup

	cfg        *S3Config
	instanceID *string

	cancel     func()
	wasWritten *atomic.Bool
}

type S3Config struct {
	Region, DataPrefix, Bucket, SqsQueueName string
	sqsQueueUrl                              *string
	CompressionType                          string
}

/*var (
	Region     = os.Getenv(`S3_REGION`)
	DataPrefix = os.Getenv(`S3_PREFIX`)
	Bucket     = os.Getenv(`S3_BUCKET`)
	SqsQueueName   = os.Getenv(`SQS_QUEUE`)
)*/

func NewS3(cfg *S3Config) Driver {
	validateInitialSettings(cfg)
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(cfg.Region),
	}))
	cfg.sqsQueueUrl = getOrCreateQueue(cfg, svc)
	s := &S3{
		sess:        svc,
		uploader:    s3manager.NewUploader(svc),
		lastFlushed: time.Now(),
		cfg:         cfg,
		instanceID:  aws.String(uuid.NewString()),
		wasWritten:  atomic.NewBool(false),
	}
	var err error
	s.w, err = s.newUploader()
	if err != nil {
		panic(err)
	}
	return s
}

func getOrCreateQueue(cfg *S3Config, sess *session.Session) *string {
	client := sqs.New(sess)
	var url *string
	r, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName:              aws.String(cfg.SqsQueueName),
		QueueOwnerAWSAccountId: nil,
	})
	if err != nil {
		if strings.Contains(err.Error(), sqs.ErrCodeQueueDoesNotExist) {
			res, err := client.CreateQueue(&sqs.CreateQueueInput{
				Attributes: map[string]*string{
					`FifoQueue`:           aws.String(`true`),
					`DeduplicationScope`:  aws.String(`messageGroup`),
					`FifoThroughputLimit`: aws.String(`perMessageGroupId`),
				},
				QueueName: aws.String(cfg.SqsQueueName),
			})
			if err != nil {
				panic(err)
			}
			url = res.QueueUrl
		} else {
			panic(err)
		}
	} else {
		url = r.QueueUrl
	}
	return url
}

func validateInitialSettings(cfg *S3Config) {
	if cfg.Region == `` {
		panic(`S3_REGION is empty`)
	}

	if cfg.Bucket == `` {
		panic(`S3_BUCKET is empty`)
	}

	if cfg.DataPrefix == `` {
		panic(`S3_PREFIX is empty`)
	}

	if cfg.SqsQueueName == `` {
		panic(`SQS_QUEUE is empty`)
	}
}

func (so *S3) newUploader() (io.WriteCloser, error) {
	r, w := io.Pipe()
	so.wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go so.upload(ctx, r)
	so.cancel = cancel
	return w, nil
}

func (so *S3) upload(ctx context.Context, r *io.PipeReader) {
	defer so.wg.Done()
	t := time.Now()
	suffix := ``
	encoding := ``
	if so.cfg.CompressionType != `` {
		suffix = fmt.Sprintf(`.%s`, so.cfg.CompressionType)
		encoding = so.cfg.CompressionType
	}
	key := fmt.Sprintf(`/%s/created_date=%s/hour=%s/%s%s`, so.cfg.DataPrefix, t.Format(`2006-01-02`), t.Format(`15`), uuid.New().String(), suffix)
	_, err := so.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:            r,
		Bucket:          aws.String(so.cfg.Bucket),
		Key:             aws.String(key),
		ContentEncoding: aws.String(encoding),
	})
	if err != nil {
		logrus.Error(err)
		return
	}
	b, _ := jsoniter.ConfigFastest.Marshal(S3SqsEvent{
		WrittenAt: time.Now().Format(time.RFC3339),
		Records: []Record{
			{
				AwsRegion: so.cfg.Region,
				S3: S3S{
					Bucket: Bucket{
						Name: so.cfg.Bucket,
					},
					Object: Object{
						Key: key,
					},
				},
			},
		}})
	uid := uuid.NewString()
	if _, err := sqs.New(so.sess).SendMessage(&sqs.SendMessageInput{
		MessageBody:            aws.String(string(b)),
		MessageDeduplicationId: aws.String(uid),
		MessageGroupId:         so.instanceID,
		QueueUrl:               so.cfg.sqsQueueUrl,
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
	Bucket Bucket `json:"Bucket"`
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
	so.wasWritten.Store(true)
	return so.w.Write(e)
}

func (so *S3) Close() error {
	if !so.wasWritten.Load() {
		so.cancel()
	}
	if err := so.w.Close(); err != nil {
		return err
	}
	so.wg.Wait()
	return nil
}
