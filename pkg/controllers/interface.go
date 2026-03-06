package controllers

import "context"

type Controller interface {
	Run(ctx context.Context, threadiness int)
	RunOnce(ctx context.Context, key string) error
	SyncOnce(ctx context.Context) error
}
