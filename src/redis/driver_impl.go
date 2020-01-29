package redis

import (
	"crypto/tls"

	"time"

	stats "github.com/lyft/gostats"
	"github.com/mediocregopher/radix/v3"
	logger "github.com/sirupsen/logrus"
)

type poolStats struct {
	connectionActive stats.Gauge
	connectionTotal  stats.Counter
	connectionClose  stats.Counter
}

func newPoolStats(scope stats.Scope) poolStats {
	ret := poolStats{}
	ret.connectionActive = scope.NewGauge("cx_active")
	ret.connectionTotal = scope.NewCounter("cx_total")
	ret.connectionClose = scope.NewCounter("cx_local_close")
	return ret
}

type poolImpl struct {
	pool  *radix.Pool
	stats poolStats
}

type connectionImpl struct {
	pool *poolImpl
}

type responseImpl struct {
	response uint32
}

func checkError(err error) {
	if err != nil {
		panic(RedisError(err.Error()))
	}
}

func (this *poolImpl) Get() Connection {
	this.stats.connectionActive.Inc()
	this.stats.connectionTotal.Inc()
	return &connectionImpl{this}
}

func (this *poolImpl) Put(c Connection) {
	this.stats.connectionActive.Dec()
}

func NewPoolImpl(scope stats.Scope, useTls bool, auth string, url string, poolSize int, overflowPoolSize int, overflowDrainPeriod time.Duration, maxNewConnPerSecond int, getTimeout time.Duration) Pool {
	logger.Warnf("connecting to redis on %s with pool size %d", url, poolSize)
	df := func(network, addr string) (radix.Conn, error) {
		opts := []radix.DialOpt{
			// Set a dial timeout on connect, read and write
			radix.DialTimeout(1 * time.Second),
		}
		if useTls {
			opts = append(opts, radix.DialUseTLS(&tls.Config{}))
		}
		if auth != "" {
			opts = append(opts, radix.DialAuthPass(auth))
		}

		return radix.Dial(network, addr, opts...)
	}

	opts := []radix.PoolOpt{
		radix.PoolConnFunc(df),
		// If the pool is empty, wait indefinitely for a new connection rather than
		// creating a new connection or returning an error.
		radix.PoolOnEmptyWait(),

		// Every period, check if the pool is full. If not, add a connection.
		radix.PoolRefillInterval(1 * time.Second),

		// If the pool is full, close the connection (rather than using an overflow pool)
		radix.PoolOnFullClose(),

		// Flush the implicit pipeline periodically but don't limit the number of commands we pipeline
		radix.PoolPipelineWindow(5*time.Millisecond, 0),

		// Number of pipelines which may be ran at once defaults to poolSize.
		radix.PoolPipelineConcurrency(poolSize),
	}

	// Enable debug traces of events on the pool
	//opts = append(opts, radix.PoolWithTrace(trace.PoolTrace{
	//// Pool has created a connection
	//ConnCreated: func(created trace.PoolConnCreated) {},

	//// Pool is about to close a connection
	//ConnClosed: func(closed trace.PoolConnClosed) {},

	//// Command is executed
	//DoCompleted: func(do trace.PoolDoCompleted) {},

	//// Pool has filled its connections
	//InitCompleted: func(init trace.PoolInitCompleted) {},
	//}))

	pool, err := radix.NewPool("tcp", url, poolSize, opts...)
	checkError(err)

	return &poolImpl{
		pool:  pool,
		stats: newPoolStats(scope)}
}

func (this *connectionImpl) PipeAppend(cmd string, args ...string) (Response, error) {
	var res uint32
	err := this.pool.pool.Do(radix.Cmd(&res, cmd, args...))
	if err != nil {
		return nil, err
	}
	return &responseImpl{res}, nil
}

func (this *responseImpl) Int() uint32 {
	return this.response
}
