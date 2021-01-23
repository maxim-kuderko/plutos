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

	mu sync.Mutex
	wg sync.WaitGroup
}

var (
	endpoints     = strings.Split(os.Getenv(`SQS_ENDPOINTS`), `,`)
	bufferSize, _ = strconv.Atoi(os.Getenv(`SQS_BUFFER`))
)

func NewSqs() Driver {
	validateInitialSettingsSQS()
	client := newSqsSender()
	s := &Sqs{client: client}
	s.buff = bytes.NewBuffer(nil)
	return s
}

func newSqsSender() *sqs.SQS {
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	client := sqs.New(svc)
	return client
}

func validateInitialSettingsSQS() {
	if len(endpoints) == 0 {
		panic(`no SQS_ENDPOINTS provided`)
	}
	if bufferSize == 0 {
		panic(`no sSQS_BUFFER provided`)
	}
	if bufferSize > 1024*256 {
		panic(`SQS_BUFFER too large`)
	}
}

func (so *Sqs) Write(e []byte) (int, error) {
	if so.buff.Len() > bufferSize {
		so.Close()
		so.client = newSqsSender()
	}
	return so.buff.Write(e)
}

func (so *Sqs) Close() error {
	u, _ := uuid.NewUUID()
	for _, endpoint := range endpoints {
		_, err := so.client.SendMessage(&sqs.SendMessageInput{
			MessageBody:            aws.String(so.buff.String()),
			MessageDeduplicationId: aws.String(u.String()),
			MessageGroupId:         aws.String(``),
			QueueUrl:               aws.String(endpoint),
		})
		log.Error().Stack().Err(err).Msg("")
	}
	return nil
}
