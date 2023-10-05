package client

import "context"

type contextKeyType string

const contextKey contextKeyType = "client"

func ContextWithClient(ctx context.Context, client *AdminClient) context.Context {
	return context.WithValue(ctx, contextKey, client)
}

func MustClientFromContext(ctx context.Context) *AdminClient {
	cl, isClient := ctx.Value(contextKey).(*AdminClient)
	if !isClient {
		panic("could not find client in the config")
	}
	return cl
}
