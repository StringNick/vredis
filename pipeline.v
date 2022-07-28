module vredis

import context

// Pipeliner is an mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// singe step.
// See https://redis.io/topics/pipelining
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Redis client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more then once.
// To avoid this: it is good idea to use reasonable bigger read/write timeouts
// depends of your batch size and/or use TxPipeline.
interface Pipeliner{
	len() int
	do(context.Context, ...string) &Cmd
	process(context.Context, Cmder) ?
	discard()
	exec(context.Context) ?[]Cmder
}

