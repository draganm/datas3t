package datas3t

import "github.com/draganm/datas3t/client"

func NewClient(baseURL string) *client.Client {
	return client.NewClient(baseURL)
}
