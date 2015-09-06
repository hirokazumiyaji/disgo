package disque

import (
	"errors"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type client struct {
	pools []*redis.Pool
}

type job struct {
	QueueName string
	Id        string
	Body      []byte
}

type AddJobOptions struct {
	Timeout   time.Duration
	Replicate time.Duration
	Delay     time.Duration
	Retry     time.Duration
	TTL       time.Duration
	MaxLen    int
	Async     bool
}

type GetJobOptions struct {
	NoHang       bool
	Timeout      time.Duration
	Count        int
	WithCounters bool
}

type QScanOptions struct {
	Count      int
	BusyLoop   bool
	MinLen     int
	MaxLen     int
	ImportRate int
}

func New(servers ...string) *client {
	c := &client{
		pools: make([]*redis.Pool, len(servers)),
	}

	for i, server := range servers {
		c.pools[i] = &redis.Pool{
			MaxIdle: 3,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}
	}
	return c
}

func (c *client) get() (redis.Conn, error) {
	for _, pool := range c.pools {
		conn := pool.Get()
		if conn.Err() == nil {
			return conn, nil
		}
	}
	return nil, errors.New("connection error")
}

func (c *client) Close() {
	for _, pool := range c.pools {
		pool.Close()
	}
}

func (c *client) AddJob(queueName string, job []byte, options AddJobOptions) (string, error) {
	conn, err := c.get()
	if err != nil {
		return "", err
	}

	args := redis.Args{queueName, job, int(options.Timeout.Nanoseconds() / 1000000)}
	if options.Replicate > 0 {
		args = args.Add("REPLICATE", options.Replicate.Seconds())
	}
	if options.Delay > 0 {
		args = args.Add("DELAY", options.Delay.Seconds())
	}
	if options.Retry > 0 {
		args = args.Add("RETRY", options.Retry.Seconds())
	}
	if options.TTL > 0 {
		args = args.Add("TTL", options.TTL.Seconds())
	}
	if options.MaxLen > 0 {
		args = args.Add("MAXLEN", options.MaxLen)
	}
	if options.Async == true {
		args = args.Add("ASYNC")
	}
	return redis.String(conn.Do("ADDJOB", args...))
}

func (c *client) GetJob(options GetJobOptions, queueNames ...string) ([]job, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	}
	args := redis.Args{}
	if options.NoHang == true {
		args = args.Add("NOHANG")
	}
	if options.Timeout.Nanoseconds() > 0 {
		args = args.Add("TIMEOUT", int(options.Timeout.Nanoseconds()/1000000))
	}
	if options.Count > 0 {
		args = args.Add("COUNT", options.Count)
	}
	if options.WithCounters == true {
		args = args.Add("WITHCOUNTERS")
	}
	args = args.Add("FROM")
	for _, queueName := range queueNames {
		args = args.Add(queueName)
	}
	reply, err := redis.Values(conn.Do("GETJOB", args...))
	if err != nil {
		return nil, err
	}

	result := make([]job, 0, len(reply))
	for _, v := range reply {
		if value, err := redis.Values(v, nil); err != nil {
			return nil, err
		} else {
			queueName, err := redis.String(value[0], nil)
			id, err := redis.String(value[1], err)
			body, err := redis.Bytes(value[2], err)
			if err != nil {
				return nil, err
			}
			result = append(result, job{QueueName: queueName, Id: id, Body: body})
		}
	}
	return result, nil
}

func (c *client) AckJob(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("ACKJOB", args...))
}

func (c *client) FastAck(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("FASTACK", args...))
}

func (c *client) NAck(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("NACK", args...))
}

func (c *client) QLen(queueName string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	return redis.Int(conn.Do("QLEN", queueName))
}

func (c *client) QPeek(queueName string, count int) ([]job, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	}

	reply, err := redis.Values(conn.Do("QPEEK", queueName, count))
	if err != nil {
		return nil, err
	}

	result := make([]job, 0, len(reply))
	for _, v := range reply {
		if value, err := redis.Values(v, nil); err != nil {
			return nil, err
		} else {
			queueName, err := redis.String(value[0], nil)
			id, err := redis.String(value[1], err)
			data, err := redis.Bytes(value[2], err)
			if err != nil {
				return nil, err
			}
			result = append(result, job{QueueName: queueName, Id: id, Body: data})
		}
	}
	return result, nil
}

func (c *client) Enqueue(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("ENQUEUE", args...))
}

func (c *client) Dequeue(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("DEQUEUE", args...))
}

func (c *client) DelJob(jobIds ...string) (int, error) {
	conn, err := c.get()
	if err != nil {
		return 0, err
	}

	args := redis.Args{}
	for _, jobId := range jobIds {
		args = args.Add(jobId)
	}
	return redis.Int(conn.Do("DELJOB", args...))
}

func (c *client) Show(jobId string) (map[string]interface{}, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	}

	reply, err := redis.Values(conn.Do("SHOW", jobId))
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i := 0; i < len(reply); i += 2 {
		if key, ok := reply[i].(string); ok {
			result[key] = reply[i+1]
		} else {
			return nil, errors.New(fmt.Sprintf("interface can not case string: %v", reply[i]))
		}
	}

	return result, nil
}

func (c *client) QScan(options QScanOptions) ([]interface{}, error) {
	conn, err := c.get()
	if err != nil {
		return nil, err
	}

	args := redis.Args{}
	if options.Count > 0 {
		args = args.Add("COUNT", options.Count)
	}
	if options.BusyLoop == true {
		args = args.Add("BUSYLOOP")
	}
	if options.MinLen > 0 {
		args = args.Add("MINLEN", options.MinLen)
	}
	if options.MaxLen > 0 {
		args = args.Add("MAXLEN", options.MaxLen)
	}
	if options.ImportRate > 0 {
		args = args.Add("IMPORTRATE", options.ImportRate)
	}

	return redis.Values(conn.Do("QSCAN", args...))
}
