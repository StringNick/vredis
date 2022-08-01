module pool

import net
import time
import sync.stdatomic
import proto
import context
import util

pub struct Conn {
	created_at time.Time
mut:
	net_conn &net.TcpConn
	pooled   bool
	used_at  i64 // atomic
	wr       &proto.Writer
	rd       &proto.Reader
pub mut:
	inited bool
}

pub fn new_conn(net_conn &net.TcpConn) Conn {
	return Conn{
		net_conn: net_conn
		created_at: time.now()
		wr: proto.new_writer(util.new_tcp_writer(net_conn))
		rd: proto.new_reader(net_conn)
	}
}

pub fn (mut cn Conn) set_net_conn(net_conn &net.TcpConn) {
	cn.net_conn = net_conn
	// TODO: reset buff
}

pub fn (mut cn Conn) used_at() time.Time {
	unix := stdatomic.load_i64(&cn.used_at)
	return time.unix2(unix, 0)
}

pub fn (mut cn Conn) set_used_at(tm time.Time) {
	stdatomic.store_i64(&cn.used_at, tm.unix_time())
}

pub fn (mut conn Conn) close() ? {
	return conn.net_conn.close()
}

pub fn (mut conn Conn) write(b []u8) ?int {
	return conn.net_conn.write(b)
}

pub fn (mut conn Conn) with_writer(ctx context.Context, timeout time.Duration) ?&proto.Writer {
	if timeout != 0 {
		conn.net_conn.set_write_deadline(time.now().add(timeout))
	}

	return conn.wr
}

// TODO: remove after resolving issue

pub fn (mut conn Conn) with_reader(ctx context.Context, timeout time.Duration) ?&proto.Reader {
	if timeout != 0 {
		conn.net_conn.set_read_deadline(time.now().add(timeout))
	}

	return conn.rd
}
