package awss

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Client struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

func (a *Client) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if a.AccessKeyID == "" || a.SecretAccessKey == "" {
		return aws.Credentials{}, fmt.Errorf("missing access key id or secret access key")
	}
	return aws.Credentials{
		AccessKeyID:     a.AccessKeyID,
		SecretAccessKey: a.SecretAccessKey,
	}, nil
}

func (a *Client) Config(ctx context.Context) (aws.Config, error) {
	return config.LoadDefaultConfig(
		ctx,
		config.WithRegion(a.Region),
		config.WithCredentialsProvider(a),
	)
}

func (a *Client) S3(ctx context.Context, bucket string) (*S3, error) {
	cfg, err := a.Config(ctx)
	if err != nil {
		return nil, err
	}
	return &S3{
		Bucket:        bucket,
		client:        s3.NewFromConfig(cfg),
		presignClient: s3.NewPresignClient(s3.NewFromConfig(cfg)),
	}, nil
}

func (a *Client) SQS(ctx context.Context, url string) (*SQS, error) {
	cfg, err := a.Config(ctx)
	if err != nil {
		return nil, err
	}
	return &SQS{
		url:          aws.String(url),
		attrNames:    []sqstypes.MessageSystemAttributeName{"All"},
		msgAttrNames: []string{"All"},
		client:       sqs.NewFromConfig(cfg),
	}, nil
}

func (a *Client) SES(ctx context.Context, mailFrom string) (*SES, error) {
	cfg, err := a.Config(ctx)
	if err != nil {
		return nil, err
	}
	return &SES{
		client: ses.NewFromConfig(cfg),
		from:   mailFrom,
	}, nil
}
