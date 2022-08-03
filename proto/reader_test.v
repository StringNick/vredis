module proto

struct BytesReader {
mut:
	prev_rune int
	i         i64
	s         []u8
}

pub fn (mut r BytesReader) len() int {
	if r.i >= i64(r.s.len) {
		return 0
	}

	return int(i64(r.s.len) - r.i)
}

pub fn (mut r BytesReader) reset(mut b []u8) {
	r = &BytesReader{
		s: b
		i: 0
		prev_rune: -1
	}
}

pub fn (mut r BytesReader) size() i64 {
	return i64(r.s.len)
}

pub fn (mut r BytesReader) read(mut b []u8) ?int {
	if r.i >= i64(r.s.len) {
		return error('eof')
	}

	r.prev_rune = -1
	n := copy(mut b, r.s[r.i..])
	r.i += i64(n)
	return n
}

fn new_bytes_buffer_reader(b []u8) &BytesReader {
	return &BytesReader{
		s: &b
		i: 0
		prev_rune: -1
	}
}

fn is_equal(a []u8, b []u8) bool {
	if a.len != b.len {
		return false
	}

	for i := 0; i < a.len; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

fn test_read_line() {
	mut b := []u8{len: 8192, init: `a`}
	b[b.len - 2] = `\r`
	b[b.len - 1] = `\n`
	buf := new_bytes_buffer_reader(b)
	mut rd := new_reader(buf)
	str := rd.read_line() or { panic(err) }
	assert is_equal(b[..b.len - 2], str.bytes())
}
