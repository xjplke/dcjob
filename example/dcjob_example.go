package main

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/gomodule/redigo/redis"
	"github.com/xjplke/dcjob"
)

var (
	poolsize    = 200
	timeout     = 240
	count       = 2 // 任务数
	workersize  = 10
	currentTest = true
	jobinterval = 20
)

//NewPool NewPool
func NewPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     poolsize,
		IdleTimeout: time.Duration(timeout) * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				fmt.Println("Dial error", err)
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func main() {
	fmt.Println("in test :", time.Now().Format("2006-01-02 15:04:05"))
	pool := NewPool()
	ch := make(chan *dcjob.Job, 100)

	dcJobs := dcjob.NewDCJobs("test2", ch, int64(jobinterval), pool)
	dcJobs.ResetJobs()

	start := time.Now()

	fmt.Println("start :", start.Format("2006-01-02 15:04:05"))

	wp := workerpool.New(workersize)
	if currentTest {
		wg := sync.WaitGroup{}
		wg.Add(count)
		for i := 0; i < count; i++ {
			t := i
			wp.Submit(func() {
				for x := 0; x < 5; x++ {
					err := dcJobs.AddJob("key"+strconv.Itoa(t), "context "+strconv.Itoa(t))
					if err == nil {
						break
					}
					fmt.Println("AddJob failed ", err, "retry = ", x)
					time.Sleep(200 * time.Millisecond)
				}
				wg.Done()
			})
		}
		wg.Wait()
	} else {
		for i := 0; i < count; i++ {
			dcJobs.AddJob("key"+strconv.Itoa(i), "context "+strconv.Itoa(i))
		}
	}
	fmt.Println("end :", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println("count :", count, " 时间: ", time.Now().Sub(start).Seconds())

	for i := 0; i < workersize/2; i++ { //一半用于任务调度
		wp.Submit(func() {
			dcJobs.Schedule()
		})
	}
	//一半用于接收消息
	var jobExectCount int32
	jobExectCount = 0
	for i := 0; i < workersize/2; i++ {
		wp.Submit(func() {
			for {
				select {
				case job := <-ch:
					atomic.AddInt32(&jobExectCount, 1)
					//jobExectCount++
					fmt.Println("job scheduled :", job, " jobExectCount :", jobExectCount)
				}
			}
		})
	}

	dcJobs.Schedule() //主协程也调度任务，同时等待
}
