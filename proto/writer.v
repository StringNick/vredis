module proto

import strconv

interface Writer_ {
mut:
	write([]u8) ?int
	write_byte(u8) ?
	write_string(string) ?int
}

pub struct Writer {
mut:
	writer  Writer_
	len_buf []u8 = []u8{cap: 64}
}

pub fn new_writer(wr Writer_) &Writer {
	return &Writer{
		writer: wr
	}
}

pub fn (mut w Writer) write_args(args []string) ? {
	w.writer.write_byte(resp_array)?

	w.write_len(args.len)?

	for arg in args {
		w.write_arg(arg)?
	}
}

fn (mut w Writer) write_len(n int) ? {
	mut len := strconv.format_int(i64(n), 10)
	w.len_buf.clear()
	w.len_buf << len.bytes()
	w.len_buf << [u8(`\r`), `\n`]
	println('write_len len=$n str="$w.len_buf.bytestr()" end')
	w.writer.write(w.len_buf)?
}

pub fn (mut w Writer) write_arg(v string) ? {
	// TODO: different argument write
	w.string(v)?
	// TODO: encoding binary marshaller
	// return error('redis: can\'t marshal $v (implement encoding.BinaryMarshaler)')
	return
}

fn (mut w Writer) string(s string) ? {
	return w.bytes(s.bytes())
}

fn (mut w Writer) bytes(b []u8) ? {
	w.writer.write_byte(resp_string)?
	w.write_len(b.len)?
	w.writer.write(b)?
	println('write string $b.bytestr() len $b.len')
	w.crlf()?
}

fn (mut w Writer) uint(n u64) ? {
	return w.bytes(strconv.format_uint(n, 10).bytes())
}

fn (mut w Writer) int(n i64) ? {
	return w.bytes(strconv.format_int(n, 10).bytes())
}

fn (mut w Writer) float(f f64) ? {
	// TODO: improve
	return w.bytes(strconv.v_sprintf('%G', f).bytes())
}

fn (mut w Writer) crlf() ? {
	w.writer.write_byte(`\r`)?
	w.writer.write_byte(`\n`)?
}
