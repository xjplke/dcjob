package dcjob

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	redigo "github.com/gomodule/redigo/redis"

	"gopkg.in/redsync.v1"
)

const (
	schKeyPrefix = "dj:sch:"
	ctxKeyPrefix = "dj:ctx:"
)

//Job job info
type Job struct {
	Key  string
	Ctx  string
	Time time.Time
}

//DCJobs for job schedule
type DCJobs struct {
	name     string
	listKey  string
	ch       chan *Job
	interval time.Duration
	pool     *redigo.Pool
	sync     *redsync.Redsync
}

//NewDCJobs 创建jobs容器
func NewDCJobs(name string, ch chan *Job, intervalSecond int64, pool *redigo.Pool) *DCJobs {
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetReportCaller(true)
	log.SetLevel(log.ErrorLevel)

	redsync := redsync.New([]redsync.Pool{pool})
	return &DCJobs{
		name:     name,
		listKey:  schKeyPrefix + name,
		ch:       ch,
		interval: time.Duration(intervalSecond) * time.Second,
		pool:     pool,
		sync:     redsync,
	}
}

//SetLoglevel SetLoglevel
func (jobs *DCJobs) SetLoglevel(level log.Level) {
	log.SetLevel(level)
}

func (jobs *DCJobs) ctxKey(key string) string {
	return ctxKeyPrefix + jobs.name + ":" + key
}

//SetInterval 重新设置任务调度间隔
func (jobs *DCJobs) SetInterval(intervalSecond int64) {
	jobs.interval = time.Duration(intervalSecond) * time.Second
}

//JobCount 获取job数量
func (jobs *DCJobs) JobCount() int64 {
	redis := jobs.pool.Get()
	defer redis.Close()

	reply, err := redigo.Int64(redis.Do("LLEN", jobs.listKey))
	if err != nil {
		log.Error("JobCount error")
		return 0
	}
	return reply
}

//ResetJobs 重置所有任务，一般不建议调用。
func (jobs *DCJobs) ResetJobs() {
	redis := jobs.pool.Get()
	defer redis.Close()

	log.Info("ResetJobs: ", jobs.name, " listKey: ", jobs.listKey, " count: ", jobs.JobCount())
	start := time.Now()
	for {
		reply, err := redigo.String(redis.Do("LPOP", jobs.listKey))
		if err != nil {
			log.Println("ResetJobs POP err:", err)
			break
		}
		redis.Do("DEL", jobs.ctxKey(reply))
	}

	diff := time.Now().Sub(start).Seconds()
	log.Info("ResetJobs ", jobs.name, " use: ", diff, " Seconds")
}

//AddJob 添加job
func (jobs *DCJobs) AddJob(key string, ctx string) error {
	redis := jobs.pool.Get()
	defer redis.Close()

	//check if key already exist
	exists, err := redigo.Int64(redis.Do("EXISTS", jobs.ctxKey(key)))
	if err != nil {
		log.Error("DCJobs ", jobs.name, "AddJob for key ", key, " error: ", err, " conn:", redis)
		return err
	}
	if exists > 0 {
		err := errors.New("Key " + key + " is exist")
		log.Error("DCJobs get key error:", err)
		return err
	}
	//获取最近一个添加任务的调度时间，及当前任务数，
	// 新任务根据当前任务数进行散列。
	len, err := redigo.Int64(redis.Do("LLEN", jobs.listKey))
	if err != nil {
		log.Error("DCJobs get list len error:", err)
		return err
	}
	log.Info("AddJob jobs len = ", len, " key:", key, " ctx:", ctx)
	jobTime := time.Now().Add(jobs.interval)

	//按理说后面两个设置应该是事务的，所以在第二个任务redis炒作处，进行了错误补偿。
	job := &Job{
		Key:  key,
		Ctx:  ctx,
		Time: jobTime,
	}

	jobJSON, err := json.Marshal(job)
	if err != nil {
		log.Error("AddJob json Marshal failed", err)
		return err
	}
	log.Info("AddJob job:", job, " jobJSON:", string(jobJSON))
	_, err = redigo.Int64(redis.Do("LPUSH", jobs.listKey, key))
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = redis.Do("SET", jobs.ctxKey(key), jobJSON)
	if err != nil { //需要从listKey中删除该key
		log.Error("", err)
		_, err = redis.Do("LREM", jobs.listKey, key)
		if err != nil {
			log.Error(err)
			return err
		}
		return err
	}
	return nil
}

//DelJob 删除job
func (jobs *DCJobs) DelJob(key string) error {
	redis := jobs.pool.Get()
	defer redis.Close()

	_, err := redis.Do("LREM", jobs.listKey, 0, key)
	if err != nil {
		log.Error(err)
		return err
	}
	_, err = redis.Do("DEL", jobs.ctxKey(key))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

//这个地方逻辑太丑陋了， 怎么简化算法？？？
func (jobs *DCJobs) scheduleOne() {
	//使用LPOPRPUSH获取一个key
	//从key中获得context，需要加锁，锁的超时时间，为interval，锁等待的超时时间为50ms(可配置)
	//如果有锁，说明当前job正在被处理，这种情况应该只在任务数远少于调度协程数才会发生。
	//	假设一种极端情况， 只有一个定时任务，但是有3个调度在处理。
	//	3个调度任务都会获取掉这个任务，其中一个在做sleep延迟。有一个锁
	//	另外两个任务应该做和处理？ 发现锁住的情况，则直接延迟一个interval后，获取下一个任务,可能还是那个任务，也可能是在interval期间新加入的任务。
	//	如果锁的超时小于interval，另外两个任务也可以在锁释放后获取到锁，然后开始该任务的调度。
	//		--- 可能的问题就是在等到期间，interval改小了，且有新添加的任务。
	//		--- 所以，锁的超时设置为任务将要等待的时间，同时获取锁的等待超时最好小于interval，比如设置为1秒。
	//		还可能有问题么？  当前所有的调度都在等待一个较长的interval，修改小interval后重新加入了新的任务？
	//				在调用set interval的时候，需要将当前等待的interval重置么？ 分布式下貌似很难重置任务。
	//				让所有的调度循环设置的较短， 如果有interval超过调度循环，则将任务放入到下一个循环。   大数据量处理会有问题。
	//等待50ms后重新调用LPOPRPUSH，当前的调度总会被其他协程很快的调度到，直接获取下一个任务。
	//获取当前时间，跟job中的时间比较，
	//	如果已经超过时间，则立即发送job到 chan
	//	如果时间未到，且等待时间小于当前interval，sleep到后再发送job到chan。
	//  如果时间未到，且等待时间大于当前interval(说明中间调小过interval)，则重设job ctx，获取下一个任务。
	redis := jobs.pool.Get()
	defer redis.Close()

	jobKey, err := redigo.String(redis.Do("RPOPLPUSH", jobs.listKey, jobs.listKey))
	if err != nil {
		log.Error("scheduleOne RPOPLPUSH error :", err)
		return
	}

	//添加job的分布式锁，
	jobKeyLock := jobs.ctxKey(jobKey) + ":lock"
	mutex := jobs.sync.NewMutex(jobKeyLock,
		redsync.SetExpiry(jobs.interval/2), //锁超时
		redsync.SetTries(1))                //不重试
	if mutex.Lock() != nil { //没有获得锁，当前在调度任务,因为任务数太少了
		//延时一个半个interval后返回
		time.Sleep(jobs.interval / 2)
		return
	}
	defer func() {
		mutex.Unlock()
	}()

	jobJSON, err := redigo.Bytes(redis.Do("GET", jobs.ctxKey(jobKey)))
	if err != nil {
		log.Error("scheduleOne error:", err)
		return
	}

	job := &Job{}
	err = json.Unmarshal(jobJSON, job)
	if err != nil {
		log.Error("Schedule Unmarshal error :", err, "jobKey: ", jobKey, " jobJSON:", jobJSON)
		return
	}
	now := time.Now()
	nextSchedule := job.Time.Add(jobs.interval)

	log.Debug("job:", job, "now:", now.Format("2006-01-02 15:04:05"), " job time:", job.Time.Format("2006-01-02 15:04:05"))
	if job.Time.Before(now) { //任务调度的时间在当前时间的前面，直接调度任务
		jobs.ch <- job
		job.Time = nextSchedule       //设置下次调度时间
		if nextSchedule.Before(now) { //说明任务有至少一个interval没有调度到，可能是算法还有问题
			log.Error("nextSchedule is before now !!  job:", job, "now:", now.Format("2006-01-02 15:04:05"),
				" nextSchedule time:", nextSchedule.Format("2006-01-02 15:04:05"))
		}
	} else if !job.Time.Before(nextSchedule) { //调度的时间超过当前interval，直接等下一个循环来调度
	} else { //调度时间未到，但是需要等待的时间小于interval，等待时间之后调度
		timeDiff := job.Time.Sub(now)
		time.Sleep(timeDiff)
		jobs.ch <- job
		job.Time = nextSchedule //设置下次调度时间
	}

	jobCtx, _ := json.Marshal(job)
	_, err = redis.Do("SET", jobs.ctxKey(job.Key), jobCtx)
	if err != nil {
		log.Error("Set job context:", err)
		return
	}
}

//Schedule 调度任务， 至少需要一个协程来调度,可以开多个协程
func (jobs *DCJobs) Schedule() {
	for {
		jobs.scheduleOne()
	}
}
