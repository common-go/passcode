package passcode

import "time"

type PasscodeSender interface {
	Send(to string, passcode string, expires time.Time, params interface{}) error
}
