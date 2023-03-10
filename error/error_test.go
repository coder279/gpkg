package error

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"testing"
)

func TestErr(t *testing.T) {
	logger, _ := zap.NewProduction()
	logger.Info("errorf", zap.Error(Errorf("%s,%d", "127.0.0.1", 80)))

	err := New("a dummy err")
	logger.Info("new", zap.Error(err))

	err = Wrap(err, "Ping timeout err")
	logger.Info("wrap", zap.Error(err))

	err = Wrapf(err, "ip: %s port:%d", "localhost", 80)
	logger.Info("wrapf", zap.Error(err))

	err = WithStack(err)
	logger.Info("withstack", zap.Error(err))

	logger.Info("wrap std", zap.Error(Wrapf(errors.New("std err"), "some err occurs")))

}
