package limiter

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/time/rate"
)

type RateWriter struct {
	writer  buf.Writer
	limiter *rate.Limiter
}

func NewRateWriter(writer buf.Writer, limiter *rate.Limiter) buf.Writer {
	return &RateWriter{
		writer:  writer,
		limiter: limiter,
	}
}

func (w *RateWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	ctx := context.Background()
	n := int(mb.Len())
	burst := w.limiter.Burst()
	for n > 0 {
		take := n
		if take > burst {
			take = burst
		}
		_ = w.limiter.WaitN(ctx, take)
		n -= take
	}
	return w.writer.WriteMultiBuffer(mb)
}

func (w *RateWriter) Close() error {
	return common.Close(w.writer)
}

func (w *RateWriter) Interrupt() {
	common.Interrupt(w.writer)
}
