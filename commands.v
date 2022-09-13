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
		return '1'
	}
	return strconv.format_int(dur / time.millisecond, 10)
}

fn format_sec(dur time.Duration) string {
	if dur > 0 && dur < time.second {
		return '1'
	}
	return strconv.format_int(dur / time.second, 10)
}

struct Cmdble_ {
	f fn (mut context.Context, mut Cmd)!
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

	c.f(mut ctx, mut cmd)!
	res := proto.scan<string>(cmd.val)!

	if res != 'OK' {
		return error(res)
	}
}

// get Redis `GET key` command, return none if empty
pub fn (mut c Cmdble_) get(mut ctx context.Context, key string) !string {
	mut cmd := new_cmd('get', key)
	c.f(mut ctx, mut cmd) or { return err }

	return proto.scan<string>(cmd.val)
}

// rpush Redis `rpush key [values...]` return llen of key
pub fn (mut c Cmdble_) rpush(mut ctx context.Context, key string, values ...string) !i64 {
	mut args := []string{len: 2, cap: 2 + values.len}
	args[0] = 'rpush'
	args[1] = key
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!

	return proto.scan<i64>(cmd.val)
}

// rpush Redis `lpush key [values...]` return llen of key
pub fn (mut c Cmdble_) lpush(mut ctx context.Context, key string, values ...string) !i64 {
	mut args := []string{len: 2, cap: 2 + values.len}
	args[0] = 'lpush'
	args[1] = key
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!

	return proto.scan<i64>(cmd.val)
}

// lrange Redis `lrange key start stop` return slice or err
pub fn (mut c Cmdble_) lrange(mut ctx context.Context, key string, start i64, stop i64) ![]string {
	mut cmd := new_cmd('lrange', key, strconv.format_int(start, 10), strconv.format_int(stop,
		10))
	c.f(mut ctx, mut cmd)!

	return proto.scan<[]string>(cmd.val)
}

// flushall Redis command `flushall`, sync by default
pub fn (mut c Cmdble_) flushall(mut ctx context.Context) ! {
	mut cmd := new_cmd('flushall')
	c.f(mut ctx, mut cmd) or { return err }
	res := proto.scan<string>(cmd.val)!

	if res != 'OK' {
		return error('wrong response: $res')
	}
}

// del Redis `del keys...` return count of deleted
pub fn (mut c Cmdble_) del(mut ctx context.Context, keys ...string) !i64 {
	mut args := []string{cap: keys.len + 1}
	args << 'del'
	args << keys

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!

	return proto.scan<i64>(cmd.val)
}

// lpop Redis `lpop key` return string or err
pub fn (mut c Cmdble_) lpop(mut ctx context.Context, key string) !string {
	mut cmd := new_cmd('lpop', key)
	c.f(mut ctx, mut cmd)!

	return proto.scan<string>(cmd.val)
}

// rpop Redis `rpop key` return string or err
pub fn (mut c Cmdble_) rpop(mut ctx context.Context, key string) !string {
	mut cmd := new_cmd('rpop', key)
	c.f(mut ctx, mut cmd)!

	return proto.scan<string>(cmd.val)!
}

// append, expire,

fn(mut c Cmdble_) auth(mut ctx context.Context, password string) ! {
	mut cmd := new_cmd('auth', password)
	c.f(mut ctx, mut cmd)!
	val := proto.scan<string>(cmd.val)!
	if val != 'OK' {
		return error('wrong result: $val')
	}
	return 
}

fn(mut c Cmdble_) auth_acl(mut ctx context.Context, username, password string) ! {
	mut cmd := new_cmd('auth', username, password)
	c.f(mut ctx, mut cmd)!
	val := proto.scan<string>(cmd.val)!
	if val != 'OK' {
		return error('wrong result: $val')
	}
	return 
}

fn(mut c Cmdble_) hello(mut ctx context.Context, ver string, username string, password string, client_name string) !map[string]proto.Any {
	mut args := []string{cap: 7}
	
	args << ['hello', ver]

	if password != '' {
		if username != '' {
			args << ['auth', username, password]
		} else {
			args << ['auth', 'default', password]
		}
	}

	if client_name != '' {
		args << client_name
	}

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!
	val := proto.scan<map[string]proto.Any>(cmd.val)!
	return val
}