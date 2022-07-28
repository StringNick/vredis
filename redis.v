module vredis

import context
import pool

[heap]
struct BaseClient {
	opt Options
mut:
	conn_pool pool.Pooler
}

pub struct Client {
	BaseClient
	Cmdble_
}

pub fn new_client(opt Options) &Client {
	b := new_base_client(opt, new_conn_pool(opt))
	mut c := &Client{
		BaseClient: b
		Cmdble_: Cmdble_{
			f: b.process
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

type ConnCallback = fn (context.Context, mut &pool.Conn) ?

fn (mut c BaseClient) get_conn(mut ctx context.Context) ?&pool.Conn {
	mut cn := c.conn_pool.get(mut ctx)?

	if cn.inited {
		return cn
	}

	c.init_conn(ctx, mut cn) or {
		c.conn_pool.remove(ctx, mut cn, err)
		return err
	}

	return cn
}

fn (mut c BaseClient) with_conn(mut ctx context.Context, f ConnCallback)? {
	mut cn := c.get_conn(mut ctx) or {return}
	mut last_err := IError(none)
	defer {
		c.release_conn(ctx, mut cn, last_err)
	}
	// TODO: remove after fix
	done := ctx.done()

	select {
		_ := <-done {}
		else {
			f(ctx, mut cn) or { last_err = err }
			return last_err
		}
	}

	d := chan IError{}
	go fn (d chan IError, ctx context.Context, mut cn &pool.Conn, f ConnCallback) {
		f(ctx, mut cn) or {
			d <- err
			return
		}
		d <- IError(none)
	}(d, ctx, mut cn, f)

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

fn (mut c BaseClient) process(mut ctx context.Context, mut cmd Cmder) ? {
	mut last_err := IError(none)

	for attempt := 0; attempt < c.opt.max_retries; attempt++ {
		retry := c.process_(mut ctx, mut cmd, attempt) or {
			last_err = err
			true
		}

		if !retry {
			return
		}
	}

	return IError(last_err)
}

fn (mut c BaseClient) process_(mut ctx context.Context, mut cmd Cmder, attempt int) ?bool {
	if attempt > 0 {
		// TODO: timeoutry
	}
	// retry_timeout := u32(1)
	c.with_conn(mut ctx, fn [mut c, mut cmd] (ctx context.Context, mut cn pool.Conn) ? {
		mut wr := cn.with_writer(ctx, c.opt.write_timeout) ?
		write_cmd(mut wr, cmd)?

		// io.new_buffered_reader({reader: io.make_reader(con)})
		// TODO: custom timeout for read
		
		mut rd := cn.with_reader(ctx, c.opt.read_timeout)?
		cmd.read_reply(mut rd)?
		return
	})?
	// TODO: retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)

	return false
}

fn (mut c BaseClient) release_conn(ctx context.Context, mut cn pool.Conn, err IError) {
	c.conn_pool.put(ctx, mut cn)

	/*
	if is_bad_conn(err, false, c.opt.addr) {
		c.conn_pool.remove(ctx, cn, err)
	} else {
		c.conn_pool.put(ctx, cn)
	}*/
}

fn (mut c BaseClient) init_conn(ctx context.Context, mut cn pool.Conn) ? {
	if cn.inited {
		return
	}

	cn.inited = true
	mut conn_pool := pool.new_single_pool_conn(c.conn_pool, cn)
	_ = new_conn(c.opt, conn_pool)

	// TODO: pipeliner
	return
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
	return &Conn{
		BaseClient: &BaseClient{
			opt: opt
			conn_pool: conn_pool
		}
	}
}
