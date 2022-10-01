module vredis

import proto

struct BaseCmd {
mut:
	args    []proto.Any
	key_pos i8
	err     string
}

pub fn (cmd BaseCmd) args() []proto.Any {
	return cmd.args
}

pub fn (mut cmd BaseCmd) set_err(err string) {
	cmd.err = err
}

pub fn (cmd BaseCmd) err() string {
	return cmd.err
}

pub struct Cmd {
	BaseCmd
mut:
	val proto.Any
}


pub struct ResultCmd<T> {
	cmd &Cmd
}

fn (r ResultCmd<T>) value() !T {
	return proto.scan<T>(r.cmd.val)
}

// TODO: proto.Any arguments
pub fn new_cmd(args ...proto.Any) &Cmd {
	return &Cmd{
		BaseCmd: BaseCmd{
			args: args
		}
	}
}

pub fn (cmd Cmd) value() proto.Any {
	return cmd.val
}

pub fn (mut cmd Cmd) set_val(val string) {
	cmd.val = val
}

pub fn (cmd Cmd) result() ?proto.Any {
	if cmd.err != '' {
		return error(cmd.err)
	}

	return cmd.val
}

fn (mut cmd Cmd) read_reply(mut rd proto.Reader) ! {
	cmd.val = rd.read_reply()!
}

fn write_cmd(mut wr proto.Writer, cmd Cmd)! {
	return wr.write_args(cmd.args())
}
