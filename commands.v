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
pub fn (mut c Cmdble_) set(mut ctx context.Context, key string, value string, expiration time.Duration) !ResultCmd<string> {
	mut args := []proto.Any{cap: 5}
	args << [proto.Any('set'), proto.Any(key), proto.Any(value)]
	if expiration > 0 {
		if use_precise(expiration) {
			args << 'px'
			args << format_ms(expiration)
		} else {
			args << 'ex'
			args << format_sec(expiration)
		}
	} else if expiration == -1 { // TODO: const
		args << 'keepttl'
	}

	mut cmd := new_cmd(...args)

	c.f(mut ctx, mut cmd)!

	return ResultCmd<string>{
		cmd: cmd
	}
}

// get Redis `GET key` command, return none if empty
pub fn (mut c Cmdble_) get(mut ctx context.Context, key string) !ResultCmd<string> {
	mut cmd := new_cmd('get', key)
	c.f(mut ctx, mut cmd)!

	return ResultCmd<string>{
		cmd: cmd
	}
}

// rpush Redis `rpush key [values...]` return llen of key
pub fn (mut c Cmdble_) rpush(mut ctx context.Context, key string, values ...proto.Any) !ResultCmd<i64> {
	mut args := []proto.Any{cap: 2 + values.len}
	args << [proto.Any('rpush'), proto.Any(key)]
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!
	return ResultCmd<i64>{
		cmd: cmd
	}
}

// rpush Redis `lpush key [values...]` return llen of key
pub fn (mut c Cmdble_) lpush(mut ctx context.Context, key string, values ...string) !ResultCmd<i64> {
	mut args := []proto.Any{len: 2, cap: 2 + values.len, init: proto.Any('')}
	args[0] = 'lpush'
	args[1] = key
	args << values

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!

	return ResultCmd<i64>{
		cmd: cmd
	}
}

// lrange Redis `lrange key start stop` return slice or err
pub fn (mut c Cmdble_) lrange(mut ctx context.Context, key string, start i64, stop i64) !ResultCmd<[]string> {
	mut cmd := new_cmd('lrange', key, strconv.format_int(start, 10), strconv.format_int(stop,
		10))
	c.f(mut ctx, mut cmd)!

	return ResultCmd<[]string>{
		cmd: cmd
	}
}

// flushall Redis command `flushall`, sync by default
pub fn (mut c Cmdble_) flushall(mut ctx context.Context) !ResultCmd<string> {
	println('calling flushall')
	mut cmd := new_cmd('flushall')
	c.f(mut ctx, mut cmd) or { return err }
	res := proto.scan<string>(cmd.val)!

	return ResultCmd<string>{
		cmd: cmd
	}
}

// del Redis `del keys...` return count of deleted
pub fn (mut c Cmdble_) del(mut ctx context.Context, keys ...string) !ResultCmd<i64> {
	mut args := []proto.Any{cap: keys.len + 1}
	args << 'del'
	for _, k in keys {
		args << k
	}

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!

	return ResultCmd<i64>{
		cmd: cmd
	}
}

// lpop Redis `lpop key` return string or err
pub fn (mut c Cmdble_) lpop(mut ctx context.Context, key string) !ResultCmd<string> {
	mut cmd := new_cmd('lpop', key)
	c.f(mut ctx, mut cmd)!

	return ResultCmd<string>{
		cmd: cmd
	}
}

// rpop Redis `rpop key` return string or err
pub fn (mut c Cmdble_) rpop(mut ctx context.Context, key string) !ResultCmd<string> {
	mut cmd := new_cmd('rpop', key)
	c.f(mut ctx, mut cmd)!
	
	return ResultCmd<string>{
		cmd: cmd
	}
}

// append, expire,

struct StatefulCmdble {
	f fn (mut context.Context, mut Cmd)!
}

fn(mut c StatefulCmdble) auth(mut ctx context.Context, password string) !ResultCmd<string> {
	mut cmd := new_cmd('auth', password)
	c.f(mut ctx, mut cmd)!
	return ResultCmd<string>{
		cmd: cmd
	}
}

fn(mut c StatefulCmdble) auth_acl(mut ctx context.Context, username string, password string) !ResultCmd<string> {
	mut cmd := new_cmd('auth', username, password)
	c.f(mut ctx, mut cmd)!
	return ResultCmd<string>{
		cmd: cmd
	}
}
// TODO: make version int, args proto.Any
fn(mut c StatefulCmdble) hello(mut ctx context.Context, ver string, username string, password string, client_name string) !ResultCmd<map[string]proto.Any> {
	mut args := []proto.Any{cap: 7}
	
	args << [proto.Any('hello'), proto.Any(ver)]

	if password != '' {
		if username != '' {
			args << [proto.Any('auth'), proto.Any(username), proto.Any(password)]
		} else {
			args << [proto.Any('auth'), proto.Any('default'), proto.Any(password)]
		}
	}

	if client_name != '' {
		args << client_name
	}

	mut cmd := new_cmd(...args)
	c.f(mut ctx, mut cmd)!
	return ResultCmd<map[string]proto.Any>{
		cmd: cmd
	}
}
// TODO: make docs and make int int
fn(mut c StatefulCmdble) select_db(mut ctx context.Context, index string) !ResultCmd<string> {
	mut cmd := new_cmd('select', index)
	c.f(mut ctx, mut cmd)!
	return ResultCmd<string>{
		cmd: cmd
	}
}