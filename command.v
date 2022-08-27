module vredis

import proto

struct BaseCmd {
mut:
	args    []string
	key_pos i8
	err     string
}

pub fn (cmd BaseCmd) args() []string {
	return cmd.args
}

pub fn (cmd BaseCmd) name() string {
	if cmd.args.len == 0 {
		return ''
	}

	return cmd.arg(0).to_lower()
}

pub fn (cmd BaseCmd) full_name() string {
	name := cmd.name()
	match name {
		'cluster', 'command' {
			if cmd.args.len == 1 {
				return name
			}
			return name + ' ' + cmd.args[1]
		}
		else {
			return name
		}
	}
}

pub fn (cmd BaseCmd) arg(pos int) string {
	if pos < 0 || pos >= cmd.args.len {
		return ''
	}

	return cmd.args[pos]
}

pub fn (cmd BaseCmd) first_key_pos() i8 {
	return cmd.key_pos
}

pub fn (mut cmd BaseCmd) set_first_key_pos(key_pos i8) {
	cmd.key_pos = key_pos
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

pub fn new_cmd(args ...string) &Cmd {
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
