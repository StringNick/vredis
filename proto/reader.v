module proto

import io
import strconv
import math
import math.big

const (
	resp_status     = u8(`+`) // +<string>\r\n
	resp_error      = u8(`-`) // -<string>\r\n
	resp_string     = u8(`$`) // $<length>\r\n<bytes>\r\n
	resp_int        = u8(`:`) // :<number>\r\n
	resp_nil        = u8(`_`) // _\r\n
	resp_float      = u8(`,`) // ,<floating-point-number>\r\n (golang float)
	resp_bool       = u8(`#`) // true: #t\r\n false: #f\r\n
	resp_blob_error = u8(`!`) // !<length>\r\n<bytes>\r\n
	resp_verbatim   = u8(`=`) // =<length>\r\nFORMAT:<bytes>\r\n
	resp_big_int    = u8(`(`) // (<big number>\r\n
	resp_array      = u8(`*`) // *<len>\r\n... (same as resp2)
	resp_map        = u8(`%`) // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	resp_set        = u8(`~`) // ~<len>\r\n... (same as Array)
	resp_attr       = u8(`|`) // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	resp_push       = u8(`>`) // ><len>\r\n... (same as Array)
)

// Not used temporarily.
// Redis has not used these two data types for the time being, and will implement them later.
// Streamed           = "EOF:"
// StreamedAggregated = '?'

const nil_value = error('redis: nil')

struct RedisError {
	code int
	msg  string
}

fn new_redis_error(str string) RedisError {
	return RedisError{
		code: 0
		msg: str
	}
}

pub fn (e RedisError) msg() string {
	return e.msg
}

pub fn (e RedisError) code() int {
	return e.code
}

pub fn parse_error_reply(line string) RedisError {
	return new_redis_error(line)
}

//------------------------------------------------------------------------------

pub struct Reader {
mut:
	rd &io.BufferedReader
	w  int // write and read offset position
	r  int
}

pub fn new_reader(rd io.Reader) &Reader {
	return &Reader{
		rd: io.new_buffered_reader(io.BufferedReaderConfig{
			reader: rd
		})
	}
}

pub fn (mut r Reader) reset(rd io.Reader) {
	r.rd = io.new_buffered_reader(io.BufferedReaderConfig{
		reader: rd
	})
	r.w = 0
	r.r = 0
}

// peek_reply_type returns the data type of the next response without advancing the Reader,
// and discard the attribute type.
pub fn (mut r Reader) peek_reply_type() ?u8 {
	mut buf := [u8(0)]
	r.rd.read(mut buf)?

	if buf[0] == proto.resp_status {
		r.discard_next()?
		return r.peek_reply_type()
	}

	return buf[0]
}

// read_line Return a valid reply, it will check the protocol or redis error,
// and discard the attribute type.
pub fn (mut r Reader) read_line() ?string {
	line := r.rd.read_line()?

	println('read_line: base line $line')

	match u8(line[0]) {
		proto.resp_error {
			println('resp err')
			return IError(parse_error_reply(line))
		}
		proto.resp_nil {
			println('resp nil')
			return proto.nil_value
		}
		proto.resp_blob_error {
			println('resp err')
			blob_error := r.read_string_reply(line)?
			return IError(new_redis_error(blob_error))
		}
		proto.resp_attr {
			println('attr')
			r.discard(line)?
			return r.read_line()
		}
		else {
			println('something else')
		}
	}

	// Compatible with RESP2
	if is_nil_reply(line) {
		return proto.nil_value
	}

	return line
}

type Empty = u8
type Any = Empty | RedisError | []Any | big.Integer | bool | f64 | i64 | map[voidptr]Any | string

pub fn (mut r Reader) read_reply() ?Any {
	line := r.read_line()?
	println('read line resp $line')

	match line[0] {
		proto.resp_status {
			s := line[1..]
			return s
		}
		proto.resp_int {
			i := strconv.parse_int(line[1..], 10, 64)?
			return i
		}
		proto.resp_float {
			f := r.read_float(line)?
			return f
		}
		proto.resp_bool {
			b := r.read_bool(line)?
			return b
		}
		proto.resp_big_int {
			i := r.read_big_int(line)?
			return i
		}
		proto.resp_string {
			s := r.read_string_reply(line)?
			return s
		}
		proto.resp_verbatim {
			v := r.read_verb(line)?
			return v
		}
		proto.resp_array, proto.resp_set, proto.resp_push {
			s := r.read_slice(line)?
			return s
		}
		proto.resp_map {
			m := r.read_map(line)?
			return m
		}
		else {
			return error('redis: can\'t parse $line')
		}
	}
}

fn (mut r Reader) read_map(line string) ?map[voidptr]Any {
	n := r.reply_len(line)?

	mut m := map[voidptr]Any{}
	for i := 0; i < n; i++ {
		k := r.read_reply()?
		v := r.read_reply() or {
			if err.msg() != proto.nil_value.msg() {
				return err
			}

			Empty(0)
		}
		m[&k] = v
	}
	return m
}

fn (mut r Reader) read_slice(line string) ?[]Any {
	n := r.reply_len(line)?

	mut val := []Any{len: n, init: Empty(0)}
	for i := 0; i < n; i++ {
		v := r.read_reply() or {
			if err.msg() != proto.nil_value.msg() {
				return err
			}

			Empty(0)
		}
		val[i] = v
	}

	return val
}

fn (mut r Reader) read_verb(line string) ?string {
	s := r.read_string_reply(line)?

	if s.len < 4 || s[3] != `:` {
		return error('redis: can\'t parse verbatim string reply: $line')
	}

	return s[4..]
}

fn (mut r Reader) read_big_int(line string) ?big.Integer {
	return big.integer_from_string(line[1..])
}

fn (mut r Reader) read_bool(line string) ?bool {
	match line[1..] {
		't' {
			return true
		}
		'f' {
			return false
		}
		else {
			return error('redis: can\'t parse bool reply: $line')
		}
	}
}

fn (mut r Reader) read_float(line string) ?f64 {
	match line[1..] {
		'inf' {
			return math.inf(1)
		}
		'-inf' {
			return math.inf(-1)
		}
		else {
			return strconv.atof64(line[1..])
		}
	}
}

// is_nil_reply detects redis.Nil of RESP2.
pub fn is_nil_reply(line string) bool {
	return line.len == 3 && (line[0] == proto.resp_string || line[0] == proto.resp_array)
		&& line[1] == `-` && line[2] == `1`
}

// discard_next read and discard the data represented by the next line.
pub fn (mut r Reader) discard_next() ? {
	line := r.read_line()?
	return r.discard(line)
}

pub fn (mut r Reader) discard(line string) ? {
	if line.len == 0 {
		return error('redis: invalid line')
	}
	match line[0] {
		proto.resp_status, proto.resp_error, proto.resp_int, proto.resp_nil, proto.resp_float,
		proto.resp_bool, proto.resp_big_int {
			return
		}
		else {}
	}

	n := r.reply_len(line)?
	match line[0] {
		proto.resp_blob_error, proto.resp_string, proto.resp_verbatim {
			mut buf := []u8{len: n + 2}
			r.rd.read(mut buf)?
			return
		}
		proto.resp_array, proto.resp_set, proto.resp_push {
			for i := 0; i < n * 2; i++ {
				r.discard_next()?
			}
			return
		}
		proto.resp_map, proto.resp_attr {
			for i := 0; i < n * 2; i++ {
				r.discard_next()?
			}
			return
		}
		else {}
	}

	return error('redis: can\'t parse $line')
}

pub fn (mut r Reader) read_string_reply(line string) ?string {
	n := r.reply_len(line)?

	mut b := []u8{len: n + 2}
	r.rd.read(mut b)?
	return b[..n].bytestr()
}

pub fn (mut r Reader) reply_len(line string) ?int {
	n := strconv.atoi(line[1..])?

	if n < -1 {
		return error('redis: invalid reply: $line')
	}

	match line[0] {
		proto.resp_string, proto.resp_verbatim, proto.resp_blob_error, proto.resp_array,
		proto.resp_set, proto.resp_push, proto.resp_map, proto.resp_attr {
			if n == -1 {
				return IError(proto.nil_value)
			}
		}
		else {}
	}

	return n
}
