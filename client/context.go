package client

import "context"

type contextKeyType string

const contextKey contextKeyType = "client"

func ContextWithClient(ctx context.Context, client *DataS3tClient) context.Context {
	return context.WithValue(ctx, contextKey, client)
}

func MustClientFromContext(ctx context.Context) *DataS3tClient {
	cl, isClient := ctx.Value(contextKey).(*DataS3tClient)
	if !isClient {
		panic("could not find client in the config")
	}
	return cl
}
