package s3util

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func GetCommonPrefixes(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	prefix string,
) ([]string, error) {

	res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		MaxKeys:   1000,
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(prefix + "/"),
		Delimiter: aws.String("/"),
	})

	if err != nil {
		return nil, fmt.Errorf("could not list common prefixes: %w", err)
	}

	prefixes := []string{}
	for _, cp := range res.CommonPrefixes {
		prefixes = append(prefixes, *cp.Prefix)
	}

	return prefixes, nil

}
