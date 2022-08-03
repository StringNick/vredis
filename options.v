module vredis

import context
import net
import time
import net.urllib
import pool

pub struct Options {
pub mut:
	// addr - host:port address
	addr     string = 'localhost:6379'
	username string
	password string
	db       int = 1

	dialer fn (context.Context, string) ?&net.TcpConn
	// max_retries - default 3; -1 disables retries.
	max_retries int = 3
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	read_timeout time.Duration = 3 * time.second
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	write_timeout time.Duration = 3 * time.second
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	idle_timeout time.Duration = 5 * time.minute

	pool_fifo       bool
	pool_size       int = 10
	min_idle_conns  int
	max_conn_age    time.Duration
	pool_timeout    time.Duration = time.second * 4
	idle_check_freq time.Duration = time.minute
}

// ParseURL parses an URL into Options that can be used to connect to Redis.
// Scheme is required.
// There are two connection types: by tcp socket and by unix socket.
// Tcp connection:
//		redis://<user>:<password>@<host>:<port>/<db_number>
// Most Option fields can be set using query parameters, with the following restrictions:
//	- field names are mapped using snake-case conversion: to set MaxRetries, use max_retries
//	- only scalar type fields are supported (bool, int, time.Duration)
//	- for time.Duration fields, values must be a valid input for time.ParseDuration();
//	  additionally a plain integer as value (i.e. without unit) is intepreted as seconds
//	- to disable a duration field, use value less than or equal to 0; to use the default
//	  value, leave the value blank or remove the parameter
//	- only the last value is interpreted if a parameter is given multiple times
//	- fields "network", "addr", "username" and "password" can only be set using other
//	  URL attributes (scheme, host, userinfo, resp.), query paremeters using these
//	  names will be treated as unknown parameters
//	- unknown parameter names will result in an error
// Examples:
//		redis://user:password@localhost:6789/3?db=1&read_timeout=6s&max_retries=2
//		is equivalent to:
//		Options{
//			addr:        "localhost:6789",
//			db:          1,               // path "/3" was overridden by "&db=1"
//			read_timeout: 6 * time.Second,
//			max_retries:  2,
//		}
pub fn parse_url(redis_url string) ?Options {
	u := urllib.parse(redis_url)?

	match u.scheme {
		'redis', 'rediss' {
			return setup_tcp_conn(u)
		}
		else {
			panic(error('we didnt support UNIX socket'))
		}
	}
}

fn (mut o Options) init() {
	o.dialer = fn (ctx context.Context, addr string) ?&net.TcpConn {
		// TODO: dial timeout


		return net.dial_tcp(addr)
	}
}

fn setup_tcp_conn(u urllib.URL) ?Options {
	mut o := Options{}

	o.username, o.password = get_user_password(u)

	o.addr = u.hostname()
	p := u.port()
	if p != '' {
		o.addr += ':' + p
	}
	// TODO:
	return o
}

fn get_user_password(u urllib.URL) (string, string) {
	mut user, mut password := '', ''
	if u.user.username != '' {
		user = u.user.username
		if u.user.password != '' {
			password = u.user.password
		}
	}

	return user, password
}

fn new_conn_pool(opt Options) &pool.ConnPool {
	return pool.new_conn_pool(pool.Options{
		dialer: fn [opt] (ctx context.Context) ?&net.TcpConn {
			return opt.dialer(ctx, opt.addr)
		}
		pool_fifo: opt.pool_fifo
		pool_size: opt.pool_size
		min_idle_conns: opt.min_idle_conns
		max_conn_age: opt.max_conn_age
		pool_timeout: opt.pool_timeout
		idle_timeout: opt.idle_timeout
		idle_check_freq: opt.idle_check_freq
	})
}
