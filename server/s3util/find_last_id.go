package s3util

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func FindLastObject(
	ctx context.Context,
	client *s3.Client,
	bucketName string,
	prefix string,
	keyToString func(key uint64) string,
) (uint64, error) {

	findMinKey := func() (uint64, error) {
		res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			MaxKeys: 1,
			Bucket:  aws.String(bucketName),
			Prefix:  aws.String(prefix),
		})
		if err != nil {
			return 0, fmt.Errorf("could not get object list: %w", err)
		}

		if res.KeyCount != 1 {
			return math.MaxUint64, nil
		}

		name := strings.TrimPrefix(*res.Contents[0].Key, prefix+"/")

		minKey, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("could not parse min key %s: %w", name, err)
		}

		return minKey, nil
	}

	keyExists := func(k uint64) (bool, error) {

		prevKey := keyToString(k - 1)

		res, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			MaxKeys:    1,
			Bucket:     aws.String(bucketName),
			Prefix:     aws.String(prefix),
			StartAfter: aws.String(prefix + "/" + prevKey),
		})
		if err != nil {
			return false, fmt.Errorf("could not get object list: %w", err)
		}

		return res.KeyCount > 0, nil

	}

	from, err := findMinKey()
	if err != nil {
		return 0, err
	}

	if from == math.MaxUint64 {
		return from, nil
	}

	to := from + 1_000_000

	for {
		exits, err := keyExists(to)
		if err != nil {
			return 0, fmt.Errorf("could not determine if key exits: %w", err)
		}

		if !exits {
			break
		}

		to *= 2
	}

	for {
		if to == from+1 {
			break
		}
		middle := (to-from)/2 + from
		middleExits, err := keyExists(middle)
		if err != nil {
			return 0, fmt.Errorf("could not determine if key exits: %w", err)
		}

		if middleExits {
			from = middle
			continue
		}

		to = middle
	}

	return from, nil

}
