package s3util

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func IterateOverKeysWithPrefix(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	prefix string,
	fn func(key string) error,
) error {
	hasNextPage := true
	var continuationToken *string

	for hasNextPage {

		res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			MaxKeys:           10_1000,
			Bucket:            aws.String(bucketName),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("could not get object list: %w", err)
		}

		for _, o := range res.Contents {
			name := *o.Key
			name = strings.TrimPrefix(name, prefix+"/")
			err = fn(name)
			if err != nil {
				return err
			}
		}

		continuationToken = res.NextContinuationToken
		hasNextPage = continuationToken != nil

	}

	return nil

}
