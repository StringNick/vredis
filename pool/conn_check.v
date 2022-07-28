module pool

import net

const (
	err_unexpected_read = error('unexpected read from socket')
)

fn conn_check(conn &net.TcpConn) ? {
	// TODO: implement check conn
	return
}