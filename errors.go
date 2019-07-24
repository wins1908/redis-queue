package redisq

import (
	"github.com/sirupsen/logrus"
)

func LogErr(err error) {
	if err != nil {
		logrus.Errorf("rmq: %s", err)
	}
}
