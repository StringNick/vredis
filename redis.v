module vredis

import context
import pool
import proto

pub const (
	nil_value = proto.nil_value
)

struct BaseClient {
	opt Options
mut:
	conn_pool pool.Pooler
}

pub struct Client {
	BaseClient
	Cmdble_
}

pub fn new_client(mut opt Options) &Client {
	opt.init()
	pool := new_conn_pool(opt)
	mut b := new_base_client(opt, pool)
	mut c := &Client{
		BaseClient: b
		Cmdble_: Cmdble_{
			f: fn[mut b] (mut ctx context.Context, mut cmd Cmd) ? {
				b.process(mut ctx, mut cmd)?
			}
		}
	}

	return c
}

fn new_base_client(opt Options, pool pool.Pooler) &BaseClient {
	return &BaseClient{
		opt: opt
		conn_pool: pool
	}
}

type ConnCallback = fn (context.Context, mut pool.Conn) ?

fn (mut c BaseClient) get_conn(mut ctx context.Context) ?pool.Conn {
	c.debug_pool()
	println('get_conn: starting get conn')
	c.debug_pool()

	mut cn := c.conn_pool.get(mut ctx)?
	println('get_conn: got connect from pool')
	c.debug_pool()

	if cn.inited {
		return cn
	}

	println('get_conn: init_conn')
	c.debug_pool()
	/*c.init_conn(ctx, mut cn) or {
		println('get_conn: err remove $err')
		c.conn_pool.remove(ctx, mut cn, err)
		return err
	}*/
	println('get_conn: init_conn_after')
	c.debug_pool()


	return cn
}

fn (mut c BaseClient) with_conn(mut ctx context.Context, f ConnCallback) ? {
	println('with_conn: init')
	c.debug_pool()

	mut cn := c.get_conn(mut ctx) or { return err }
	mut last_err := IError(none)
	println('with_conn: get connect successfull')
	c.debug_pool()
	defer {
		println('with_conn: releasing connection')
		c.debug_pool()

		c.release_conn(ctx, mut cn, error('123'))
	}
	// TODO: remove after fix
	done := ctx.done()
	eprintln('with_conn: started with conn')

	match ctx {
		context.EmptyContext {
			eprintln('with_conn: turning conn')
			f(ctx, mut cn) or { 
				last_err = err
				return err
			}
			println('with_conn: successfully executed')
			c.debug_pool()
			return
		} else {
			
		}
	}

	d := chan IError{}
	
	eprintln('with_conn: try to go spawn')
	go fn (d chan IError, ctx context.Context, mut cn pool.Conn, f ConnCallback) {
		f(ctx, mut cn) or {
			d <- err
			return
		}
		d <- IError(none)
	}(d, ctx, mut &cn, f)

	select {
		_ := <-done {
			cn.close() or {}
			_ := <-d
			last_err = ctx.err()
			return last_err
		}
		last_err = <-d {
			return last_err
		}
	}

	return
}

fn (mut c BaseClient) process(mut ctx context.Context, mut cmd Cmd) ? {
	mut last_err := IError(none)

	println('process: start processing')

	for attempt := 0; attempt < c.opt.max_retries; attempt++ {
		retry := c.process_(mut ctx, mut cmd, attempt) or {
			last_err = err
			// TODO: second bool param

			if should_retry(err, false) {
				continue
			}

			return last_err
		}

		if !retry {
			return
		}
	}

	return last_err
}

fn (mut c BaseClient) process_(mut ctx context.Context, mut cmd Cmd, attempt int) ?bool {
	if attempt > 0 {
		// TODO: timeoutry
	}

	eprintln('process_: processing cmd: $attempt')
	// retry_timeout := u32(1)
	c.with_conn(mut ctx, fn [mut c, mut cmd] (ctx context.Context, mut cn pool.Conn) ? {
		eprintln('with conn anon fn')
		mut wr := cn.with_writer(ctx, c.opt.write_timeout)?
		write_cmd(mut wr, cmd)?

		// io.new_buffered_reader({reader: io.make_reader(con)})
		// TODO: custom timeout for read

		mut rd := cn.with_reader(ctx, c.opt.read_timeout)?
		cmd.read_reply(mut rd)?
		return
	}) or {
		println('with conn err $err')
		return err
	}
	// TODO: retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)

	return false
}

fn (c BaseClient) debug_pool() {
	pl := c.conn_pool
	if pl is pool.ConnPool {
		addr := voidptr(pl as &pool.ConnPool)
			println('debug pool address $addr')
	}
}

fn (mut c BaseClient) release_conn(ctx context.Context, mut cn pool.Conn, err IError) {
	println('release connection')
    
	c.debug_pool()
	
	c.conn_pool.put(ctx, mut cn)

	/*
	if is_bad_conn(err, false, c.opt.addr) {
		c.conn_pool.remove(ctx, cn, err)
	} else {
		c.conn_pool.put(ctx, cn)
	}*/
}

fn (c BaseClient) init_conn(ctx context.Context, mut cn pool.Conn) ? {
	println('init_conn: pooled=$cn.inited')
	c.debug_pool()
	if cn.inited {
		return
	}

	cn.inited = true
	println('init_conn: new_signle_pool_conn')
	c.debug_pool()
	//mut conn_pool := pool.new_single_pool_conn(c.conn_pool, cn)
//	_ = new_conn(c.opt, conn_pool)
	println('init_conn: new_conn')
	c.debug_pool()
	// TODO: pipeliner
}

/*
fn (mut c BaseClient) with_conn(ctx context.Context, f fn(context.Context, &pool.Conn) ?) {
	cn := c.get_conn(ctx)?
	mut res_err := IError(none)
	defer {
		c.release_conn(ctx, cn, res_err)
	}

	done := ctx.done()


}
*/

pub struct Conn {
	BaseClient
	Cmdble_
}

fn new_conn(opt Options, conn_pool pool.Pooler) &Conn {
	println('redis_new_conn')
	return &Conn{
		BaseClient: &BaseClient{
			opt: opt
			conn_pool: conn_pool
		}
	}
}
