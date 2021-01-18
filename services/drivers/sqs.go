package drivers

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Sqs struct {
	client      *sqs.SQS
	currentSize int
	lastFlushed time.Time

	buff *bytes.Buffer

	pool sync.Pool
	mu   sync.Mutex
	wg   sync.WaitGroup
}

var (
	endpoints     = strings.Split(os.Getenv(`SQS_ENDPOINTS`), `,`)
	bufferSize, _ = strconv.Atoi(os.Getenv(`SQS_BUFFER`))
)

func NewSqs() io.WriteCloser {
	validateInitialSettingsSQS()
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	client := sqs.New(svc)
	s := &Sqs{client: client, pool: sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(nil)
		},
	}}
	s.buff = s.pool.Get().(*bytes.Buffer)
	go s.periodicFlush()
	return s
}
func validateInitialSettingsSQS() {
	if len(endpoints) == 0 {
		panic(`no sqs endpoints provided`)
	}
	if bufferSize == 0 {
		panic(`no sqs buffer size provided`)
	}
	if bufferSize > 1024*256 {
		panic(`sqs buffer too large`)
	}
}
func (so *Sqs) periodicFlush() {
	ticker := time.NewTicker(time.Second)
	maxTimeBetweenFlushes := time.Duration(maxTime) * time.Second
	for range ticker.C {
		so.mu.Lock()
		if time.Since(so.lastFlushed) > maxTimeBetweenFlushes && so.currentSize > 0 {
			if err := so.flush(); err != nil {
				log.Err(err)
			}
		}
		so.mu.Unlock()

	}
}
func (so *Sqs) Write(e []byte) (int, error) {
	so.mu.Lock()
	defer so.mu.Unlock()
	if len(e)+so.currentSize > bufferSize && so.currentSize > 0 {
		so.flush()
	}
	so.buff.Write(e)
	so.currentSize += len(e)
	return len(e), nil
}

func (so *Sqs) flush() error {
	so.currentSize = 0
	so.lastFlushed = time.Now()
	tmp := so.buff
	so.buff = so.pool.Get().(*bytes.Buffer)
	so.wg.Add(1)
	go func() {
		defer func() {
			tmp.Reset()
			so.pool.Put(tmp)
			so.wg.Done()
		}()
		u, _ := uuid.NewUUID()
		for _, endpoint := range endpoints {
			_, err := so.client.SendMessage(&sqs.SendMessageInput{
				MessageBody:            aws.String(tmp.String()),
				MessageDeduplicationId: aws.String(u.String()),
				MessageGroupId:         aws.String(``),
				QueueUrl:               aws.String(endpoint),
			})
			log.Err(err)
		}

	}()
	return nil
}

func (so *Sqs) Close() error {
	so.mu.Lock()
	err := so.flush()
	so.wg.Wait()
	fmt.Println(`closed`)
	return err
}
