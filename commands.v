module vredis

import context
import time
import strconv
import proto

pub const (
	keep_ttl = -1
)

fn use_precise(dur time.Duration) bool {
	return dur < time.second || dur % time.second != 0
}

fn format_ms(dur time.Duration) string {
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

fn format_sec(dur time.Duration) string {
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
	f fn (mut context.Context, mut Cmd) ?
}

// set Redis `set key` command
pub fn (mut c Cmdble_) set(mut ctx context.Context, key string, value string, expiration time.Duration) ! {
	mut args := []string{len: 3, cap: 5}
	args[0] = 'set'
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if use_precise(expiration) {
			args << ['px', format_ms(expiration)]
		} else {
			args << ['ex', format_sec(expiration)]
		}
	} else if expiration == -1 { // TODO: const
		args << 'keepttl'
	}

	mut cmd := new_cmd(...args)

	c.f(mut ctx, mut cmd) or { return err }

	cmd_res := proto.scan_type_string(cmd.val)!
	if cmd_res != 'OK' {
		return error(cmd_res)
	}
	return
}

// get Redis `GET key` command, return none if empty
pub fn (mut c Cmdble_) get(mut ctx context.Context, key string) ?string {
	mut cmd := new_cmd('get', key)
	c.f(mut ctx, mut cmd) or { return err }

	res := proto.scan_type_string(cmd.val) or { return err }

	return res
}

// rpush Redis `rpush key [values...]` return llen of key
pub fn (mut c Cmdble_) rpush(mut ctx context.Context, key string, values ...string) ?i64 {
	mut args := []string{len: 2, cap: 2 + values.len}
	args[0] = 'rpush'
	args[1] = key
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd) or { return err }

	res := proto.scan_type_int(cmd.val) or { return err }

	return res
}

// rpush Redis `lpush key [values...]` return llen of key
pub fn (mut c Cmdble_) lpush(mut ctx context.Context, key string, values ...string) ?i64 {
	mut args := []string{len: 2, cap: 2 + values.len}
	args[0] = 'lpush'
	args[1] = key
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd) or { return err }

	res := proto.scan_type_int(cmd.val) or { return err }

	return res
}

// lrange Redis `lrange key start stop` return slice or err
pub fn (mut c Cmdble_) lrange(mut ctx context.Context, key string, start i64, stop i64) ?[]string {
	mut cmd := new_cmd('lrange', key, strconv.format_int(start, 10), strconv.format_int(stop,
		10))
	c.f(mut ctx, mut cmd) or { return err }

	res := proto.scan_type_string_slice(cmd.val) or { return err }

	return res
}

// flushall Redis command `flushall`, sync by default
pub fn (mut c Cmdble_) flushall(mut ctx context.Context) ! {
	mut cmd := new_cmd('flushall')
	c.f(mut ctx, mut cmd) or { return err }
	cmd_res := proto.scan_type_string(cmd.val)!

	if cmd_res != 'OK' {
		return error('wrong response: $cmd_res')
	}
}

// del Redis `del keys...` return count of deleted
pub fn (mut c Cmdble_) del(mut ctx context.Context, keys ...string) ?i64 {
	mut args := []string{cap: keys.len + 1}
	args << 'del'
	args << keys

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd) or { return err }

	res := proto.scan_type_int(cmd.val) or { return err }

	return res
}

// lpop Redis `lpop key` return string or err
pub fn (mut c Cmdble_) lpop(mut ctx context.Context, key string) ?string {
	mut cmd := new_cmd('lpop', key)
	c.f(mut ctx, mut cmd) or {
		return err
	}

	res := proto.scan_type_string(cmd.val) or {return err}
	return res
}