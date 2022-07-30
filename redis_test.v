module vredis
import context
import time
//key string, value string, expiration time.Duration

fn test_q() {
	//new_client()
	mut opt := Options{}
	mut cl := new_client(mut opt)
	mut ctx := context.Context(context.EmptyContext(0))
	val := 'bar'
	key := 'test'
	
	res := cl.set(mut ctx, key, val, time.hour).result() or {
		eprintln('command error result: $err')
		return
	}

	res1 := cl.get(mut ctx, key).result() or {
		if err == nil_value {
			eprintln('key not exist')
			return
		}
		eprintln('command get error: $err')
			return
	}
	assert res1 == val
}
