package disque

import (
	"bytes"
	"testing"
	"time"
)

const DummyJobId = "DI0f0c644ffca14064ced6f8f997361a5c0af65ca305a0SQ"

var DummyJob = []byte("dummyjob")

func TearDownAddJob(c *client) {
	timeout, _ := time.ParseDuration("1ms")
	for {
		_, err := c.GetJob(GetJobOptions{Timeout: timeout}, "test:addjob")
		if err != nil {
			return
		}
	}
}

func TestAddJob(t *testing.T) {
	c := New("127.0.0.1:7711")
	defer TearDownAddJob(c)

	_, err := c.AddJob("test:addjob", DummyJob, AddJobOptions{})
	if err != nil {
		t.Error(err)
	}

	replicate, _ := time.ParseDuration("1s")
	delay, _ := time.ParseDuration("1s")
	retry, _ := time.ParseDuration("1s")
	ttl, _ := time.ParseDuration("5s")
	_, err = c.AddJob(
		"test:addjob",
		DummyJob,
		AddJobOptions{
			Replicate: replicate,
			Delay:     delay,
			Retry:     retry,
			TTL:       ttl,
			MaxLen:    10,
			Async:     true,
		},
	)
	if err != nil {
		t.Error(err)
	}
}

func TestGetJob(t *testing.T) {
	c := New("127.0.0.1:7711")
	queueName := "test:getjob"

	timeout, _ := time.ParseDuration("1ms")
	_, err := c.GetJob(GetJobOptions{Timeout: timeout}, queueName)
	if err == nil {
		t.Errorf("unexcepted err is null")
	} else if err.Error() != "redigo: nil returned" {
		t.Errorf("unexcepted err %v", err.Error())
	}

	jobId, err := c.AddJob(queueName, DummyJob, AddJobOptions{})
	if err != nil {
		t.Error(err)
	}

	jobs, err := c.GetJob(GetJobOptions{}, queueName)
	if err != nil {
		t.Error(err)
	}
	if len(jobs) != 1 {
		t.Errorf("%v != %v", len(jobs), 1)
	}
	job := jobs[0]
	if job.Id != jobId {
		t.Errorf("%v != %v", job.Id, jobId)
	}
	if job.QueueName != queueName {
		t.Errorf("%v != %v", job.QueueName, queueName)
	}
	if bytes.Equal(job.Body, DummyJob) == false {
		t.Errorf("%v != %v", job.Body, DummyJob)
	}
}

func TestAckJob(t *testing.T) {
	c := New("127.0.0.1:7711")

	count, err := c.AckJob(DummyJobId)
	if err != nil {
		t.Error(err)
	}
	if count != 0 {
		t.Errorf("unexcepted count %v", count)
	}
}

func TestFastAck(t *testing.T) {
	c := New("127.0.0.1:7711")
	count, err := c.FastAck(DummyJobId)
	if err != nil {
		t.Error(err)
	}
	if count != 0 {
		t.Errorf("unexcepted count %v", count)
	}
}

//func TestNAck(t *testing.T) {
//	c := New("127.0.0.1:7711")
//	_, err := c.NAck(DummyJobId)
//	if err != nil {
//		t.Error(err)
//	}
//
//	jobId, err := c.AddJob("test:nack", DummyJob, AddJobOptions{})
//	if err != nil {
//		t.Error(err)
//	}
//
//	count, err := c.NAck(jobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if count != 1 {
//		t.Error("unexcepted count %v", count)
//	}
//
//	jobIds := make([]string, 10)
//	for i := 0; i < 10; i++ {
//		jobId, err := c.AddJob("test:nack", DummyJob, AddJobOptions{})
//		if err != nil {
//			t.Error(err)
//		}
//		jobIds = append(jobIds, jobId)
//	}
//
//	count, err = c.NAck(jobIds...)
//	if err != nil {
//		t.Error(err)
//	}
//	if count != 10 {
//		t.Error("unexcepted count %v", count)
//	}
//}
//
//func TestQLen(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	length, err := c.QLen("test:qlen")
//	if err != nil {
//		t.Error(err)
//	}
//	if length != 0 {
//		t.Error("unexpected length %v", length)
//	}
//
//	for i := 0; i < 10; i++ {
//		_, err = c.AddJob("test:qlen", DummyJob, AddJobOptions{})
//		if err != nil {
//			t.Error(err)
//		}
//	}
//
//	length, err = c.QLen("test:qlen")
//	if err != nil {
//		t.Error(err)
//	}
//	if length != 10 {
//		t.Error("unexpected length %v", length)
//	}
//}
//
//func TestQPeek(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	jobs, err := c.QPeek("test:qpeek", 1)
//	if err != nil {
//		t.Error(err)
//	}
//	if jobs == nil {
//		t.Error("unexcepted result is nil")
//	}
//	if len(jobs) != 0 {
//		t.Error("unexcepted result %v", len(jobs))
//	}
//}
//
//func TestEnqueue(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	count, err := c.Enqueue(DummyJobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if count != 0 {
//		t.Error("unexpected count %v", count)
//	}
//}
//
//func TestDequeue(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	count, err := c.Dequeue(DummyJobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if count != 0 {
//		t.Error("unexpected count %v", count)
//	}
//}
//
//func TestDelJob(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	r, err := c.DelJob(DummyJobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if r != 0 {
//		t.Error("unexpected count %v", r)
//	}
//
//	jobId, err := c.AddJob("test:deljob", DummyJob, AddJobOptions{})
//	if err != nil {
//		t.Error(err)
//	}
//	r, err = c.DelJob(jobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if r != 1 {
//		t.Error("unexpected count %v", r)
//	}
//
//	jobIds := make([]string, 0, 10)
//	for i := 0; i < 10; i++ {
//		jobId, err = c.AddJob("test:deljob", DummyJob, AddJobOptions{})
//		if err != nil {
//			t.Error(err)
//		}
//		jobIds = append(jobIds, jobId)
//	}
//
//	r, err = c.DelJob(jobIds...)
//	if err != nil {
//		t.Error(err)
//	}
//	if r != 10 {
//		t.Error("unexpected count %v", r)
//	}
//}
//
//func TestShow(t *testing.T) {
//	c := New("127.0.0.1:7711")
//
//	result, err := c.Show(DummyJobId)
//	if err != nil {
//		t.Error(err)
//	}
//	if result == nil {
//		t.Error("unexcepted show result nil")
//	}
//}
