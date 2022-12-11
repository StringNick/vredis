module proto

import strconv

interface Writer_ {
mut:
	write([]u8) !int
	write_byte(u8) !
	write_string(string) !int
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

pub fn (mut w Writer) write_args(args []Any) ! {
	w.writer.write_byte(resp_array) or { return err }

	w.write_len(args.len) or { return err }

	for arg in args {
		w.write_arg(arg) or { return err }
	}
}

fn (mut w Writer) write_len(n int) ! {
	mut len := strconv.format_int(i64(n), 10)
	w.len_buf.clear()
	w.len_buf << len.bytes()
	w.len_buf << [u8(`\r`), `\n`]
	w.writer.write(w.len_buf) or { return err }
}

// TODO: implement another type for writing argument
pub fn (mut w Writer) write_arg(v Any) ! {
	match v {
		bool {
			if v {
				return w.int(1)
			}
			return w.int(0)
		}
		f64 {
			return w.float(v)
		}
		i64 {
			return w.int(v)
		}
		int {
			return w.int(i64(v))
		}
		string {
			return w.string(v)
		}
		else {
			return error('redis: can\'t marshal ${v}')
		}
	}
}

fn (mut w Writer) string(s string) ! {
	return w.bytes(s.bytes())
}

// TODO: implement bytes support
fn (mut w Writer) bytes(b []u8) ! {
	w.writer.write_byte(resp_string) or { return err }
	w.write_len(b.len)!
	w.writer.write(b) or { return err }
	w.crlf()!
}

// TODO: implement uint
fn (mut w Writer) uint(n u64) ! {
	return w.bytes(strconv.format_uint(n, 10).bytes())
}

fn (mut w Writer) int(n i64) ! {
	return w.bytes(strconv.format_int(n, 10).bytes())
}

fn (mut w Writer) float(f f64) ! {
	// TODO: improve
	return w.bytes(strconv.v_sprintf('%G', f).bytes())
}

fn (mut w Writer) crlf() ! {
	w.writer.write_byte(`\r`) or { return err }
	w.writer.write_byte(`\n`) or { return err }
}
