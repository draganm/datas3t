package bucket

import (
	bucketadd "github.com/draganm/datas3t/cmd/datas3t/bucket/add"
	bucketlist "github.com/draganm/datas3t/cmd/datas3t/bucket/list"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "bucket",
		Usage: "Manage S3 bucket configurations",
		Subcommands: []*cli.Command{
			bucketadd.Command(),
			bucketlist.Command(),
		},
	}
}