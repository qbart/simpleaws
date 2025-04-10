package awss

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/aws-sdk-go/aws"
)

type Mailer interface {
	SendEmail(ctx context.Context, to []string, replyTo []string, email *Email) error
}

type SES struct {
	client *ses.Client
	from   string
}

type Email struct {
	Subject string
	Text    string
	HTML    string
}

func (s *SES) SendEmail(ctx context.Context, to []string, replyTo []string, email *Email) error {
	input := &ses.SendEmailInput{
		Destination: &types.Destination{
			ToAddresses:  to,
			CcAddresses:  nil,
			BccAddresses: nil,
		},
		Source:           aws.String(s.from),
		ReplyToAddresses: replyTo,
		Message: &types.Message{
			Subject: &types.Content{
				Charset: aws.String("UTF-8"),
				Data:    aws.String(email.Subject),
			},
			Body: &types.Body{
				Html: &types.Content{
					Charset: aws.String("UTF-8"),
					Data:    aws.String(email.HTML),
				},
				Text: &types.Content{
					Charset: aws.String("UTF-8"),
					Data:    aws.String(email.Text),
				},
			},
		},
	}
	_, err := s.client.SendEmail(ctx, input)
	if err != nil {
		return err
	}
	return nil
}
