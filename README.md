# disgo
go client for disque

## Install

```
$ go get github.com/hirokazumiyaji/disgo/disque
```

## Usage

```
import "github.com/hirokazumiyaji/disgo/disque"

c := disque.New("127.0.0.1:7711")

// ADDJOB
jobId, err := c.AddJob("queue", []byte("body"), AddJobOptions{})

replicate, _ := time.ParseDuration("1s")
delay, _ := time.ParseDuration("1s")
retry, _ := time.ParseDuration("1s")
ttl, _ := time.ParseDuration("5s")
jobId, err := c.AddJob(
    "queue",
    []byte("body"),
    AddJobOptions{
        Replicate: replicate,
        Delay:     delay,
        Retry:     retry,
        TTL:       ttl,
        MaxLen:    10,
        Async:     true,
    },
)

// GETJOB
jobs, err := c.GetJob(GetJobOptions{}, jobId)
```
