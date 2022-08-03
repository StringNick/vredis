module pool

import time
import sync
import context
import sync.stdatomic
import net

pub const (
	err_closed       = error('redis: client is closed')
	err_pool_timeout = error('redis: connection pool timeout')
)

pub struct Options {
mut:
	dialer          fn (context.Context) ?&net.TcpConn
	pool_fifo       bool
	pool_size       int
	min_idle_conns  int
	max_conn_age    time.Duration
	pool_timeout    time.Duration
	idle_timeout    time.Duration
	idle_check_freq time.Duration
}

pub struct ConnPool {
mut:
	conns_mu &sync.Mutex // mutex

	opt Options

	dial_errors_num u64 // atomic

	last_dial_error IError
	queue           chan bool
	conns           []Conn
	idle_conns      []Conn
	closed_ch       chan bool
	pool_size       int
	idle_conns_len  int
	closed_         u64
}

interface Pooler {
mut:
	new_conn(context.Context) ?Conn
	close_conn(mut Conn) ?
	get(mut context.Context) ?Conn
	put(context.Context, mut Conn)
	remove(context.Context, mut Conn, IError)
	len() int
	idle_len() int
	close() ?
}

pub fn new_conn_pool(opt Options) &ConnPool {
	println('new_conn_pool')
	mut p := &ConnPool{
		opt: opt
		last_dial_error: none
		queue: chan bool{cap: opt.pool_size}
		conns: []Conn{cap: opt.pool_size}
		idle_conns: []Conn{cap: opt.pool_size}
		closed_ch: chan bool{}
		conns_mu: sync.new_mutex()
	}

	p.conns_mu.init()
	p.conns_mu.@lock()
	p.check_min_idle_conns()
	p.conns_mu.unlock()

	return p
}

fn (mut p ConnPool) check_min_idle_conns() {
	if p.opt.min_idle_conns == 0 {
		return
	}

	println('check_min_idle_conns')

	for p.pool_size < p.opt.pool_size && p.idle_conns_len < p.opt.min_idle_conns {
		p.pool_size++
		p.idle_conns_len++

		go fn (mut p ConnPool) {
			p.add_idle_conn() or {
				if err != pool.err_closed {
					p.conns_mu.@lock()

					p.pool_size--
					p.idle_conns_len--
					p.conns_mu.unlock()
				}

				return
			}
		}(mut p)
	}
}

fn (mut p ConnPool) add_idle_conn() ? {
	println('add_idle_conn: start')
	mut cn := p.dial_conn(context.todo(), true)?
	p.conns_mu.@lock()

	defer {
		p.conns_mu.unlock()
	}

	if p.closed() {
		cn.close() or {}
		return pool.err_closed
	}

	println('add_idle_conn: successfully added new connection')
	p.conns << cn
	p.idle_conns << cn
}

fn (mut c ConnPool) dial_conn(ctx context.Context, pooled bool) ?Conn {
	println('dialin connect')
	if c.closed() {
		return pool.err_closed
	}
	c.conns_mu.@lock()
	mut dial_errors_num := c.dial_errors_num
	c.conns_mu.unlock()
	if dial_errors_num > u64(c.opt.pool_size) {
		return c.last_dial_error
	}

	net_conn := c.opt.dialer(ctx) or {
		println('dial error $err')

		c.conns_mu.@lock()
		c.last_dial_error = err
		c.dial_errors_num++
		dial_errors_num = c.dial_errors_num
		c.conns_mu.unlock()

		if dial_errors_num == u64(c.opt.pool_size) {
			go c.try_dial()
		}
		return err
	}

	println('successfully dialed conn')

	mut cn := new_conn(net_conn)
	cn.pooled = pooled
	return cn
}

fn (mut c ConnPool) closed() bool {
	return stdatomic.load_u64(&c.closed_) == 1
}

fn (mut c ConnPool) try_dial() {
	println('try_dial: started')
	for {
		if c.closed() {
			return
		}

		mut conn := c.opt.dialer(context.background()) or {
			c.last_dial_error = err
			time.sleep(time.second)
			continue
		}
		
		stdatomic.store_u64(&c.dial_errors_num, 0)
		conn.close() or {}
		return
	}
}

fn (mut p ConnPool) new_conn_(ctx context.Context, pooled bool) ?Conn {
	println('new_conn_: new conn initiated')
	mut cn := p.dial_conn(ctx, pooled)?
	println('new_conn_: dialed new conn')
	p.conns_mu.@lock()

	defer {
		p.conns_mu.unlock()
	}

	if p.closed() {
		cn.close() or {}
		return pool.err_closed
	}

	p.conns << cn
	if pooled {
		if p.pool_size >= p.opt.pool_size {
			cn.pooled = false
		} else {
			p.pool_size++
		}
	}

	println('new_conn_: return new cn')

	return cn
}

pub fn (mut p ConnPool) new_conn(ctx context.Context) ?Conn {
	return p.new_conn_(ctx, false)
}

// get returns existed connection from the pool or creates a new one.
pub fn (mut p ConnPool) get(mut ctx context.Context) ?Conn {
	println('pool_conn: get')
	if p.closed() {
		return pool.err_closed
	}

	p.wait_turn(mut ctx)?
	println('pool_conn: waited turn')
	for {
		//	time.sleep(time.second)
		p.conns_mu.@lock()
		mut cn := p.pop_idle(ctx) or {
			p.conns_mu.unlock()
			if err == error('pop_idle: empty') {
				println('get: break and trying new conn')
				break
			}
			println('get: pop_idle return err $err')
			return err
		}
		p.conns_mu.unlock()
		println('successfully poped connection')
		/*
		TODO: isHealthy
		if p.is_stale_conn(mut cn) {
			println('stale conn')
			p.close_conn_(mut cn) or {}
			continue
		}*/

		return cn
	}

	newcn := p.new_conn_(ctx, true) or {
		p.free_turn()
		return err
	}
	return newcn
}

fn (mut p ConnPool) get_turn() {
	p.queue.try_push(true)
}

fn (mut p ConnPool) wait_turn(mut ctx context.Context) ? {
	done := ctx.done()
	select {
		_ := <-done {
			return ctx.err()
		}
		p.queue <- true {
			return
		}
		time.hour * 1 {
			return pool.err_pool_timeout
		}
	}
}

fn (mut p ConnPool) free_turn() {
	_ := <-p.queue
}

fn (mut p ConnPool) pop_idle(ctx context.Context) ?Conn {
	if p.closed() {
		return pool.err_closed
	}
	println('pop_idle: init $p.idle_conns.len')
	n := p.idle_conns.len
	if n == 0 {
		println('pop_idle: empty')
		return error('pop_idle: empty')
	}

	defer {
		p.idle_conns_len--
		p.check_min_idle_conns()
	}

	if p.opt.pool_fifo {
		cn := p.idle_conns.first()
		p.idle_conns.delete(0)
		return cn
	} else {
		cn := p.idle_conns.last()
		p.idle_conns.delete_last()
		return cn
	}
}

pub fn (mut p ConnPool) put(ctx context.Context, mut cn Conn) {
	// TODO: check bufferred content pool/pool
	println("put: init $cn.pooled")
	if !cn.pooled {
		p.remove(ctx, mut cn, none)
		return
	}

	addr := voidptr(p)
	println('lock1 $addr')

	p.conns_mu.@lock()
	println('locked')
	l := p.idle_conns.len
	println("put: idle_conns len $l")
	p.idle_conns << cn
	p.idle_conns_len++
	p.conns_mu.unlock()

	p.free_turn()
}

fn (mut p ConnPool) remove(ctx context.Context, mut cn Conn, reason IError) {
	p.remove_conn_with_lock(cn)
	p.free_turn()
	p.close_conn_(mut cn) or {}
}

pub fn (mut p ConnPool) close_conn(mut cn Conn) ? {
	p.remove_conn_with_lock(cn)
	return p.close_conn_(mut cn)
}

fn (mut p ConnPool) remove_conn_with_lock(cn Conn) {
	p.conns_mu.@lock()

	p.remove_conn(cn)
	p.conns_mu.unlock()
}

fn (mut p ConnPool) remove_conn(cn Conn) {
	for i, c in p.conns {
		if voidptr(c.net_conn) == voidptr(cn.net_conn) {
			p.conns.delete(i)
			if cn.pooled {
				p.pool_size--
				p.check_min_idle_conns()
			}
		}
	}
}

fn (mut p ConnPool) close_conn_(mut cn Conn) ? {
	// TODO: onclose hook
	return cn.close()
}

pub fn (mut p ConnPool) len() int {
	p.conns_mu.@lock()

	n := p.conns.len
	p.conns_mu.unlock()

	return n
}

pub fn (mut p ConnPool) idle_len() int {
	p.conns_mu.@lock()

	n := p.idle_conns_len
	p.conns_mu.unlock()

	return n
}

pub fn (mut p ConnPool) filter(f fn (Conn) bool) ? {
	p.conns_mu.@lock()

	defer {
		p.conns_mu.unlock()
	}

	mut first_error := IError(none)
	for mut cn in p.conns {
		if f(cn) {
			p.close_conn_(mut cn) or {
				if first_error == IError(none) {
					first_error = err
				}
			}
		}
	}
	if first_error == IError(none) {
		return
	} else {
		return first_error
	}
}

pub fn (mut p ConnPool) close() ? {
	if p.closed() {
		return pool.err_closed
	}

	stdatomic.store_u64(&p.closed_, 1)
	p.closed_ch.close()

	mut first_err := IError(none)
	p.conns_mu.@lock()

	for i := 0; i < p.conns.len; i++ {
		mut cn := p.conns[i]
		p.close_conn_(mut cn) or {
			if first_err == IError(none) {
				first_err = err
			}
		}
	}

	p.conns = []Conn{}
	p.pool_size = 0
	p.idle_conns = []Conn{}
	p.idle_conns_len = 0
	p.conns_mu.unlock()

	return first_err
}

fn (mut p ConnPool) is_stale_conn(mut cn Conn) bool {
	if p.opt.idle_timeout == 0 && p.opt.max_conn_age == 0 {
		mut res := true
		conn_check(cn.net_conn) or { res = false }
		return res
	}

	now := time.now()
	if p.opt.idle_timeout > 0 && now - (cn.used_at()) >= p.opt.idle_timeout {
		return true
	}
	if p.opt.max_conn_age > 0 && now - (cn.created_at) >= p.opt.max_conn_age {
		return true
	}

	mut res := true
	conn_check(cn.net_conn) or { res = false }

	return res
}
