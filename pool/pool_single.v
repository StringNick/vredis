module pool

import context

const err_default_sticky = error('sticky')

pub struct SinglePoolConn {
mut:
	pool       Pooler
	cn         Conn
	sticky_err IError = pool.err_default_sticky
}

pub fn new_single_pool_conn(pool Pooler, cn Conn) &SinglePoolConn {
	return &SinglePoolConn{
		pool: pool
		cn: cn
	}
}

pub fn (mut p SinglePoolConn) new_conn(ctx context.Context) !Conn {
	return p.pool.new_conn(ctx)
}

pub fn (mut p SinglePoolConn) close_conn(mut cn Conn) ! {
	return p.pool.close_conn(mut cn)
}

pub fn (mut p SinglePoolConn) get(mut ctx context.Context) !Conn {
	if p.sticky_err != pool.err_default_sticky {
		return p.sticky_err
	}

	return p.cn
}

pub fn (mut p SinglePoolConn) put(ctx context.Context, mut cn Conn) {}

pub fn (mut p SinglePoolConn) remove(ctx context.Context, mut cn Conn, reason IError) {
	p.sticky_err = reason
}

pub fn (mut p SinglePoolConn) close() ! {
	p.sticky_err = err_closed
	return
}

pub fn (mut p SinglePoolConn) len() int {
	return 0
}

pub fn (mut p SinglePoolConn) idle_len() int {
	return 0
}
