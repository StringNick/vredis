module util

import net

pub struct TcpWriter {
	net.TcpConn
}

pub fn new_tcp_writer(conn &net.TcpConn) &TcpWriter {
	return &TcpWriter{
		TcpConn: conn
	}
}

pub fn (mut w TcpWriter) write_byte(b u8) ? {
	w.write([b])?
}
