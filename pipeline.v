module vredis

import proto
import context
import sync

pub struct Pipeline {
	Cmdble_
	StatefulCmdble
mut:
	exec_ fn (mut context.Context, mut []Cmd) !
	mu    sync.Mutex
	cmds  []Cmd
}

fn (mut p Pipeline) init() {
	p.Cmdble_ = Cmdble_{
		f: fn [mut p] (mut ctx context.Context, mut cmd Cmd) ! {
			p.process(ctx, mut cmd)!
		}
	}
	p.StatefulCmdble = StatefulCmdble{
		f: fn [mut p] (mut ctx context.Context, mut cmd Cmd) ! {
			p.process(ctx, mut cmd)!
		}
	}
}

pub fn (mut p Pipeline) len() int {
	p.mu.@lock()
	ln := p.cmds.len
	p.mu.unlock()

	return ln
}

pub fn (mut p Pipeline) do(mut ctx context.Context, args ...proto.Any) &Cmd {
	mut cmd := new_cmd(...args)
	p.process(ctx, mut cmd) or {}
	return cmd
}

pub fn (mut p Pipeline) process(ctx context.Context, mut cmd Cmd) ! {
	p.mu.@lock()
	p.cmds << cmd
	p.mu.unlock()
}

pub fn (mut p Pipeline) discard() {
	p.mu.@lock()
	p.cmds.clear()
	p.mu.unlock()
}

pub fn (mut p Pipeline) exec(mut ctx context.Context) ![]Cmd {
	p.mu.@lock()
	defer {
		p.mu.unlock()
	}

	if p.cmds.len == 0 {
		return p.cmds
	}

	mut cmds := p.cmds.clone()
	p.cmds.trim(0)
	p.exec_(mut ctx, mut cmds)!
	return cmds
}

pub fn (mut p Pipeline) pipelined(mut ctx context.Context, fun fn (Pipeline) !) ![]Cmd {
	fun(p)!

	return p.exec(mut ctx)
}

pub fn (mut p Pipeline) tx_pipelined(mut ctx context.Context, fun fn (Pipeline) !) ![]Cmd {
	return p.pipelined(mut ctx, fun)
}
