module main

import cd

fn main() {
	mut opt := Options{}
	mut cl := new_client(mut opt)
	mut ctx := context.Context(context.EmptyContext(0))
	cmd := cl.set(mut ctx, 'test', 'bar', time.hour)
	eprintln('lient $cl cmd $cmd')
}