module vredis

import context
import strconv
import time

__global (
	cl = setup()
)

fn setup() &Client {
	mut opt := Options{
		addr: '127.0.0.1:6379'
		min_idle_conns: 10
		pool_size: 100
		password: 'test123'
	}

	mut cl := new_client(mut opt)
	time.sleep(5 * time.second)
	mut ctx := context.todo()
	res := cl.flushall(mut ctx) or {
		panic('flushall error $err')
	}

	val := res.value() or {
		panic(err)
	}

	if val != 'OK' {
		panic('flushall setup not ok $res.value()')
	}
	return cl
}


fn multithread_rpush(key string, i i64){
	mut ctx := context.todo()

	val := strconv.format_int(i, 10)
	println('[thread#$val] rpush init..')
	cl.rpush(mut ctx, key, val) or {
		println('[thread#$val] rpush error $err')
	}
}

fn test_multithreading() ! {
	mut ctx := context.todo()

	count := 10
	key := 'test_list_thread'
	mut threads := []thread{}
	for i := 0; i < count; i ++ {
		threads << go multithread_rpush(key, i64(i))
	}
	threads.wait()

	lrange := cl.lrange(mut ctx, key, 0, -1)!.value()!
	assert lrange.len == count
	println('waited')
}

fn test_get_set()! {
	mut ctx := context.todo()
	val := 'bar'
	key := 'test_key'

	cl.set(mut ctx, key, val, time.hour) or {
		println('test_get_set: $err')
		return err
	}

	res := cl.get(mut ctx, key)!.value()!
	assert res == val

	del_count := cl.del(mut ctx, key)!.value()!
	assert del_count == 1
}

fn test_lpush_lrange() ! {
	mut ctx := context.todo()
	key, val, val1 := 'test_list', 'val', 'val1'

	push_count := cl.rpush(mut ctx, key, val, val1)!.value()!
	assert push_count == 2

	lrange := cl.lrange(mut ctx, key, 0, -1)!.value()!
	assert lrange.len == 2

	assert lrange[0] == val && lrange[1] == val1

	del_count := cl.del(mut ctx, key)!.value()!
	assert del_count == 1
}

fn test_lpop_rpop() ! {
	mut ctx := context.todo()
	key, val, val1, val2 := 'test_list', 'val', 'val1', 'val2'

	push_count := cl.rpush(mut ctx, key, val, val1, val2)!.value()!
	assert push_count == 3

	res := cl.lpop(mut ctx, key)!.value()!
	assert res == val

	res1 := cl.rpop(mut ctx, key)!.value()!
	assert res1 == val2

	del_count := cl.del(mut ctx, key)!.value()!
	assert del_count == 1
}

fn test_flushall() ! {
	mut ctx := context.todo()
	val := 'bar'
	key := 'test_key'

	cl.set(mut ctx, key, val, time.hour)!

	cl.flushall(mut ctx)!

	res := cl.get(mut ctx, key) or {
		assert err.msg() == nil_value.msg()
		return
	}
}