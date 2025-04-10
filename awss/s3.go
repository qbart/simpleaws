package awss

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

type Files interface {
	GetObject(ctx context.Context, fromKey string, w io.Writer) error
	PutObjectMulti(ctx context.Context, objects []PutObjectInput) error
}

type S3 struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	Bucket        string
}

func (files *S3) GetPresignURL(ctx context.Context, fromKey string, duration time.Duration) (*v4.PresignedHTTPRequest, error) {
	req, err := files.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(files.Bucket),
		Key:    aws.String(fromKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = duration
	})
	if err != nil {
		return nil, fmt.Errorf("error creating get request: %v", err)
	}
	return req, nil
}

func (files *S3) GetObject(ctx context.Context, fromKey string, w io.Writer) error {
	url, err := files.GetPresignURL(ctx, fromKey, 15*time.Minute)
	if err != nil {
		return err
	}

	resp, err := http.Get(url.URL)
	if err != nil {
		return fmt.Errorf("error downloading file(%s): %v", fromKey, err)
	}
	defer resp.Body.Close()

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return fmt.Errorf("error saving downloaded file: %v", err)
	}

	return nil
}

type PutObjectInput struct {
	Key         string
	ContentType string
	Reader      io.Reader
}

func (files *S3) PutObjectMulti(ctx context.Context, objects []PutObjectInput) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, obj := range objects {
		obj := obj // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			_, err := files.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(files.Bucket),
				Key:         aws.String(obj.Key),
				ContentType: aws.String(obj.ContentType),
				Body:        obj.Reader,
			})
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (files *S3) PutObject(ctx context.Context, object PutObjectInput) error {
	_, err := files.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(files.Bucket),
		Key:         aws.String(object.Key),
		ContentType: aws.String(object.ContentType),
		Body:        object.Reader,
	})
	return err
}
