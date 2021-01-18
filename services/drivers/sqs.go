package drivers

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Sqs struct {
	client *sqs.SQS

	buff *bytes.Buffer

	pool sync.Pool
	mu   sync.Mutex
	wg   sync.WaitGroup
}

var (
	endpoints     = strings.Split(os.Getenv(`SQS_ENDPOINTS`), `,`)
	bufferSize, _ = strconv.Atoi(os.Getenv(`SQS_BUFFER`))
)

func NewSqs() Driver {
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

func (so *Sqs) Write(e []byte) (int, error) {
	return so.buff.Write(e)
}

func (so *Sqs) Flush() error {
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
			log.Error().Stack().Err(err).Msg("")
		}

	}()
	return nil
}

func (so *Sqs) Close() error {
	so.Flush()
	so.wg.Wait()
	return nil
}
