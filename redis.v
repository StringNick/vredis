module vredis

import context
import pool
import proto

pub const (
	nil_value = proto.nil_value
)

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

pub fn new_client(mut opt Options) &Client {
	opt.init()
	pool := new_conn_pool(opt)
	mut b := new_base_client(opt, pool)
	mut c := &Client{
		conn_pool: pool
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

pub fn (mut c BaseClient) pipeline() &Pipeline {
	mut pipe := Pipeline{
		exec_: c.process_pipeline
	}
	pipe.init()
	return &pipe
}

fn (mut c BaseClient) process_pipeline(mut ctx context.Context, mut cmds []Cmd) ! {
	unsafe {
		c.general_process_pipeline(mut ctx, mut cmds, c.pipeline_process_cmds)!
	}
}

fn (mut c BaseClient) general_process_pipeline(mut ctx context.Context, mut cmds []Cmd, p fn (context.Context, mut pool.Conn, mut []Cmd) !bool) ! {
	c.with_conn(mut ctx, fn [p, mut cmds] (ctx context.Context, mut cn pool.Conn) ! {
		p(ctx, mut cn, mut cmds)!
	}) or { return err }
}

fn write_cmds(mut wr proto.Writer, cmds []Cmd) ! {
	for _, cmd in cmds {
		write_cmd(mut wr, cmd)!
	}
}

pub fn (mut c BaseClient) pipeline_process_cmds(ctx context.Context, mut cn pool.Conn, mut cmds []Cmd) !bool {
	mut wr := cn.with_writer(ctx, c.opt.write_timeout)!
	write_cmds(mut wr, cmds)!

	// io.new_buffered_reader({reader: io.make_reader(con)})
	mut rd := cn.with_reader(ctx, c.opt.read_timeout)!
	pipeline_read_cmds(mut rd, mut cmds)!
	return true
}

fn pipeline_read_cmds(mut rd proto.Reader, mut cmds []Cmd) ! {
	for _, mut cmd in mut cmds {
		cmd.read_reply(mut rd)!
	}
}

fn (mut c BaseClient) get_conn(mut ctx context.Context) ?pool.Conn {
	// println('get_conn: starting get conn')

	mut cn := c.conn_pool.get(mut ctx) or { return err }
	// println('get_conn: got connect from pool')

	if cn.inited {
		return cn
	}

	// println('get_conn: init_conn')
	c.init_conn(mut ctx, mut cn) or {
		// println('get_conn: err remove $err')
		c.conn_pool.remove(ctx, mut cn, err)
		return err
	}
	// println('get_conn: init_conn_after')

	return cn
}

type ConnCallback = fn (context.Context, mut pool.Conn) !

fn (mut c BaseClient) with_conn(mut ctx context.Context, f ConnCallback) ? {
	mut cn := c.get_conn(mut ctx) or { return err }
	mut last_err := IError(none)
	defer {
		c.release_conn(ctx, mut cn, error('123'))
	}
	// TODO: remove after fix
	done := ctx.done()

	match ctx {
		context.EmptyContext {
			f(ctx, mut cn) or {
				last_err = err
				return err
			}
			return
		}
		else {}
	}

	d := chan IError{}

	// eprintln('with_conn: try to go spawn')
	spawn fn (d chan IError, ctx context.Context, mut cn pool.Conn, f ConnCallback) {
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

fn (mut c BaseClient) process(mut ctx context.Context, mut cmd Cmd) ! {
	mut last_err := IError(none)

	// println('process: start processing')

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

fn (mut c BaseClient) process_(mut ctx context.Context, mut cmd Cmd, attempt int) !bool {
	if attempt > 0 {
		// TODO: timeoutry
	}

	// retry_timeout := u32(1)
	c.with_conn(mut ctx, fn [mut c, mut cmd] (ctx context.Context, mut cn pool.Conn) ! {
		mut wr := cn.with_writer(ctx, c.opt.write_timeout)!
		write_cmd(mut wr, cmd)!

		// io.new_buffered_reader({reader: io.make_reader(con)})
		// TODO: custom timeout for read
		mut rd := cn.with_reader(ctx, c.opt.read_timeout)!
		cmd.read_reply(mut rd)!
		return
	}) or {
		// println('with conn err $err')
		return err
	}
	// TODO: retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)

	return false
}

fn (mut c BaseClient) release_conn(ctx context.Context, mut cn pool.Conn, err IError) {
	// println('release connection')

	c.conn_pool.put(ctx, mut cn)

	/*
	if is_bad_conn(err, false, c.opt.addr) {
		c.conn_pool.remove(ctx, cn, err)
	} else {
		c.conn_pool.put(ctx, cn)
	}*/
}

fn (c BaseClient) init_conn(mut ctx context.Context, mut cn pool.Conn) ! {
	if cn.inited {
		return
	}

	cn.inited = true

	username := c.opt.username
	password := c.opt.password
	// println('init_conn: new_signle_pool_conn')
	mut conn_pool := pool.new_single_pool_conn(c.conn_pool, cn)
	mut conn := new_conn(c.opt, conn_pool)
	conn.hello(mut ctx, '3', username, password, '') or { return err }

	if password != '' {
		if username != '' {
			conn.auth_acl(mut ctx, username, password)!
		} else {
			conn.auth(mut ctx, password)!
		}
	}

	// println('init_conn: new_conn')
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
	StatefulCmdble
}

fn new_conn(opt Options, conn_pool pool.Pooler) &Conn {
	// println('redis_new_conn')
	mut b := new_base_client(opt, conn_pool)

	return &Conn{
		conn_pool: conn_pool
		BaseClient: b
		Cmdble_: Cmdble_{
			f: b.process
		}
		StatefulCmdble: StatefulCmdble{
			f: b.process
		}
	}
}
