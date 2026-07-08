package generic

import (
	"strconv"
	"time"

	"frostdb/internal/kernel/context"
	"frostdb/internal/resp/types"
)

// TimeCommand implements TIME — returns [unix_seconds, microseconds].
type TimeCommand struct{}

func (c TimeCommand) Name() string { return "TIME" }

func (c TimeCommand) Execute(ctx *context.Context, args []types.Value) types.Value {
	nowMs := ctx.NowMs()
	secs := nowMs / 1000
	micros := (nowMs % 1000) * 1000

	// If it's a script time, we don't have true microseconds, but the tests
	// usually don't strictly assert the microsecond precision to be exact,
	// just that the time returned doesn't change during the script.
	if ctx.ScriptTimeMs == 0 {
		now := time.Now()
		secs = now.Unix()
		micros = now.UnixMicro() - secs*1_000_000
	}


	return types.NewArray([]types.Value{
		types.NewString(strconv.FormatInt(secs, 10)),
		types.NewString(strconv.FormatInt(micros, 10)),
	})
}
