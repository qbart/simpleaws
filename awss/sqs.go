package awss

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Message struct {
	ID  string
	Key string
}

type Queue interface {
	Receive(ctx context.Context, messages []types.Message) (int, error)
	Acknowledge(ctx context.Context, id string) error
}

type SQS struct {
	url          *string
	attrNames    []types.MessageSystemAttributeName
	msgAttrNames []string
	client       *sqs.Client
}

func (q *SQS) Receive(ctx context.Context, messages []types.Message) (int, error) {
	maxMessages := min(10, len(messages))

	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    q.url,
		MessageSystemAttributeNames: q.attrNames,
		MessageAttributeNames:       q.msgAttrNames,
		MaxNumberOfMessages:         int32(maxMessages),
		VisibilityTimeout:           30,
		WaitTimeSeconds:             20,
	})
	if err != nil {
		return 0, fmt.Errorf("[sqs] failed to receive messages from Queue: %v", err)
	}
	n := copy(messages, out.Messages)

	return n, nil
}

func (q *SQS) Acknowledge(ctx context.Context, id string) error {
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      q.url,
		ReceiptHandle: aws.String(id),
	})
	return err
}
