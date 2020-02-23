package passcode

import (
	"context"
	"time"
)

type PasscodeSender interface {
	Send(ctx context.Context, to string, passcode string, expires time.Time, params interface{}) error
}
