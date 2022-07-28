module vredis

import context
import time
import strconv

pub const (
	keep_ttl = -1
)

fn use_precise(dur time.Duration) bool {
	return dur < time.second || dur % time.second != 0
}

fn format_ms(ctx context.Context, dur time.Duration) string {
	if dur > 0 && dur < time.millisecond {
		/*
		internal.Logger.Printf(
			ctx,
			"specified duration is %s, but minimal supported value is %s - truncating to 1ms",
			dur, time.Millisecond,
		)*/
		return '1'
	}
	return strconv.format_int(dur / time.millisecond, 10)
}

fn format_sec(ctx context.Context, dur time.Duration) string {
	if dur > 0 && dur < time.second {
		/*
		internal.Logger.Printf(
			ctx,
			"specified duration is %s, but minimal supported value is %s - truncating to 1s",
			dur, time.Second,
		)*/
		return '1'
	}
	return strconv.format_int(dur / time.second, 10)
}

struct Cmdble_ {
	f fn (mut context.Context, mut Cmder)?
}

pub fn (mut c Cmdble_) set(mut ctx context.Context, key string, value string, expiration time.Duration) &StatusCmd {
	mut args := []string{len: 3, cap: 5}
	args[0] = 'set'
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if use_precise(expiration) {
			args << ['px', format_ms(ctx, expiration)]
		} else {
			args << ['ex', format_sec(ctx, expiration)]
		}
	} else if expiration == -1 { // TODO: const
		args << 'keepttl'
	}

	mut cmd := new_status_cmd(ctx, ...args)
	mut t := Cmder(cmd)
	c.f(mut ctx, mut t) or {}
	return cmd
}
