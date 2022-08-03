module vredis

import context
import time

__global (
	cl = setup()
)

fn setup() &Client {
	mut opt := Options{
		addr: 'localhost:6379'
		min_idle_conns: 3
	}

	mut cl := new_client(mut opt)
	time.sleep(time.second)
	return cl
}

fn test_get_set() ? {
	mut ctx := context.todo()
	val := 'bar'
	key := 'test_key'

	cl.set(mut ctx, key, val, time.hour) or {
		println('redis_test: $err')
		return err
	}

	res := cl.get(mut ctx, key)?
	assert res == val

	del_count := cl.del(mut ctx, key)?
	assert del_count == 1
}

fn test_lpush_lrange() ? {
	mut ctx := context.todo()
	key, val, val1 := 'test_list', 'val', 'val1'

	push_count := cl.rpush(mut ctx, key, val, val1)?
	assert push_count == 2

	lrange := cl.lrange(mut ctx, key, 0, -1) or { return err }
	assert lrange.len == 2

	assert lrange[0] == val && lrange[1] == val1

	del_count := cl.del(mut ctx, key)?
	assert del_count == 1
}

fn test_flushall() ? {
	mut ctx := context.todo()
	val := 'bar'
	key := 'test_key'

	cl.set(mut ctx, key, val, time.hour) or { return err }

	cl.flushall(mut ctx) or { return err }

	res := cl.get(mut ctx, key) or {
		assert err.msg() == nil_value.msg()
		''
	}
}
