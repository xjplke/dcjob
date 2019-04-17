package dcjob

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gammazero/workerpool"
	"github.com/gomodule/redigo/redis"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	poolsize    = 200
	timeout     = 240
	count       = 20 // 任务数
	workersize  = 10
	currentTest = true
	jobinterval = 20
)

var dialCount = 0

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
			dialCount++
			fmt.Println("Dial called dialCount = ", dialCount)
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

func TestDCJobs(t *testing.T) {
	fmt.Println("before Convey:", time.Now().Format("2006-01-02 15:04:05"))
	log.SetLevel(log.DebugLevel)
	Convey("创建jobs", t, func() {
		fmt.Println("in test :", time.Now().Format("2006-01-02 15:04:05"))
		pool := NewPool()
		ch := make(chan *Job, 100)

		dcJobs := NewDCJobs("test2", ch, int64(jobinterval), pool)
		dcJobs.ResetJobs()
		So(dcJobs, ShouldNotEqual, nil)

		So(dcJobs.JobCount(), ShouldEqual, 0)
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
		//		So(dcJobs.AddJob("key2", "context 2"), ShouldNotEqual, nil)
		So(dcJobs.JobCount(), ShouldEqual, count)

		dcJobs.DelJob("key" + strconv.Itoa(4))
		dcJobs.DelJob("key" + strconv.Itoa(5))
		So(dcJobs.JobCount(), ShouldEqual, count-2)
		fmt.Println("before quit :", time.Now().Format("2006-01-02 15:04:05"))

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
	})
}
