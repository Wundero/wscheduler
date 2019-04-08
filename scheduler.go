package wscheduler

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Scheduler struct {
	tasks      map[int]*Task
	timer      *time.Ticker
	defTimeout time.Duration
	nextID     int
	queue      chan *Task
	_lock      *sync.Mutex
	workers    map[*worker]chan int
}

type Status int

const (
	WAITING Status = 0
	RUNNING Status = 1
	DONE    Status = 2
	ERR     Status = 3
	TIMEOUT Status = 4
)

type worker struct {
	id int
}

type Task struct {
	function    func(chan int)
	Time        time.Time
	ID          int
	Timeout     time.Duration
	Close       func(...interface{}) bool
	running     bool
	subscribers []chan Status
	status      Status
	closed      bool
	Args        []interface{}
	_lock       *sync.Mutex
}

func (t *Task) GetStatus() Status {
	t._lock.Lock()
	defer t._lock.Unlock()
	return t.status
}

func min(b, a int) int {
	if b > a {
		return a
	}
	return b
}

func NewPoolTimeoutScheduler(workers int, timeout time.Duration) *Scheduler {
	workers = min(workers, 10000)
	sch := &Scheduler{
		tasks:      make(map[int]*Task),
		nextID:     0,
		timer:      nil,
		_lock:      &sync.Mutex{},
		queue:      make(chan *Task),
		workers:    make(map[*worker]chan int),
		defTimeout: timeout,
	}
	for i := 0; i < workers; i++ {
		sch.workers[&worker{id: i}] = make(chan int)
	}
	return sch
}

func NewPoolScheduler(workers int) *Scheduler {
	return NewPoolTimeoutScheduler(workers, time.Minute*5)
}

func (t *Task) OnStatus(fn func(), stx Status) {
	ch := t.Subscribe()
	go func() {
		for {
			st := <-ch
			if st == stx {
				fn()
			}
		}
	}()
}

func (t *Task) OnError(fn func()) {
	t.OnStatus(fn, ERR)
}

func (t *Task) OnTimeout(fn func()) {
	t.OnStatus(fn, TIMEOUT)
}

func (t *Task) OnComplete(fn func()) {
	t.OnStatus(fn, DONE)
}

func (t *Task) Subscribe() chan Status {
	ch := make(chan Status)
	t.subscribers = append(t.subscribers, ch)
	return ch
}

func NewScheduler() *Scheduler {
	return NewPoolScheduler(8)
}

func str(in int) string {
	return strconv.Itoa(in)
}

func (s *Scheduler) ScheduleCustom(task *Task, fn func(...interface{})) error {
	s._lock.Lock()
	if len(s.tasks) > 50000 {
		s._lock.Unlock()
		return fmt.Errorf("too many tasks already scheduled")
	}
	tid := s.nextID
	s.nextID += 1
	task.ID = tid
	s.tasks[tid] = task
	task.function = func(ch chan int) {
		fn(task.Args)
		ch <- 0
	}
	task._lock = &sync.Mutex{}
	task.running = false
	task.closed = false
	task.status = WAITING
	task.subscribers = make([]chan Status, 0)
	s._lock.Unlock()
	return nil
}

func (s *Scheduler) ScheduleRecurringUntil(fn func(...interface{}), inst time.Time,
	duration time.Duration, predicate func(...interface{}) bool, a ...interface{}) (*Task, error) {
	s._lock.Lock()
	if len(s.tasks) > 50000 {
		s._lock.Unlock()
		return nil, fmt.Errorf("too many tasks already scheduled")
	}
	tid := s.nextID
	s.nextID = s.nextID + 1
	var task *Task
	cl := func(args ...interface{}) bool {
		task.Time = task.Time.Add(duration)
		return predicate(args...)
	}
	task = &Task{
		function: func(ch chan int) {
			fn(task.Args...)
			ch <- 0
		},
		ID:          tid,
		Time:        inst,
		Close:       cl,
		running:     false,
		status:      WAITING,
		closed:      false,
		Timeout:     s.defTimeout,
		subscribers: make([]chan Status, 0),
		Args:        a,
		_lock:       &sync.Mutex{},
	}
	s.tasks[tid] = task
	s._lock.Unlock()
	return task, nil
}

func (s *Scheduler) ScheduleOnce(fn func(...interface{}), inst time.Time, a ...interface{}) (*Task, error) {
	s._lock.Lock()
	if len(s.tasks) > 50000 {
		s._lock.Unlock()
		return nil, fmt.Errorf("too many tasks already scheduled")
	}
	tid := s.nextID
	s.nextID = s.nextID + 1
	cl := func(...interface{}) bool { return true }
	var task *Task
	task = &Task{
		function: func(ch chan int) {
			fn(task.Args...)
			ch <- 0
		},
		ID:          tid,
		Time:        inst,
		Close:       cl,
		running:     false,
		status:      WAITING,
		Timeout:     s.defTimeout,
		subscribers: make([]chan Status, 0),
		closed:      false,
		Args:        a,
		_lock:       &sync.Mutex{},
	}
	s.tasks[tid] = task
	s._lock.Unlock()
	return task, nil
}

func (s *Scheduler) ScheduleRecurring(fn func(...interface{}), inst time.Time, interval time.Duration,
	a ...interface{}) (*Task, error) {
	s._lock.Lock()
	if len(s.tasks) > 50000 {
		s._lock.Unlock()
		return nil, fmt.Errorf("too many tasks already scheduled")
	}
	tid := s.nextID
	s.nextID = s.nextID + 1
	var task *Task
	cl := func(...interface{}) bool {
		task.Time = task.Time.Add(interval)
		return false
	}
	task = &Task{
		function: func(ch chan int) {
			fn(task.Args...)
			ch <- 0
		},
		ID:          tid,
		Time:        inst,
		Close:       cl,
		running:     false,
		status:      WAITING,
		Timeout:     s.defTimeout,
		subscribers: make([]chan Status, 0),
		closed:      false,
		_lock:       &sync.Mutex{},
		Args:        a,
	}
	s.tasks[tid] = task
	s._lock.Unlock()
	return task, nil
}

func closer(task *Task) bool {
	err := make(chan int, 1)
	timer := time.NewTicker(task.Timeout)
	ch := make(chan bool)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r)
				task.status = ERR
				err <- 0
			}
		}()
		ch <- task.Close(task.Args...)
	}()
	select {
	case cl := <-ch:
		return cl
	case <-timer.C:
		task.status = TIMEOUT
		return true
	case <-err:
		return true
	}
}

func run(task *Task) bool {
	err := make(chan int, 1)
	timer := time.NewTicker(task.Timeout)
	ch := make(chan int)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				task.status = ERR
				fmt.Println(r)
				err <- 0
			}
		}()
		task.function(ch)
	}()
	select {
	case <-ch:
		return false
	case <-timer.C:
		task.status = TIMEOUT
		return true
	case <-err:
		return true
	}
}

func (w *worker) work(queue chan *Task, quit chan int) {
	for {
		select {
		case task := <-queue:
			task._lock.Lock()
			if task.closed || task.running {
				task._lock.Unlock()
				continue
			}
			task.status = RUNNING
			task.running = true
			if run(task) {
				task.running = false
				task.closed = true
				task._lock.Unlock()
				continue
			}
			task.running = false
			task.closed = closer(task)
			if task.closed && task.status == RUNNING {
				task.status = DONE
			} else if task.status == RUNNING {
				task.status = WAITING
			}
			task._lock.Unlock()
		case <-quit:
			return
		}
	}
}

func (s *Scheduler) schedule() {
	s._lock.Lock()
	for _, task := range s.tasks {
		if task.Time.Before(time.Now()) || task.Time.Equal(time.Now()) {
			select {
			case s.queue <- task:
			default:
			}
		}
	}
	run := make([]int, 0)
	for _, task := range s.tasks {
		task._lock.Lock()
		if task.closed {
			run = append(run, task.ID)
		}
		task._lock.Unlock()
	}
	for _, id := range run {
		tx := s.tasks[id]
		for _, sub := range tx.subscribers {
			select {
			case sub <- tx.status:
				continue
			default:
				continue
			}
		}
		delete(s.tasks, id)

	}
	s._lock.Unlock()
}

func (s *Scheduler) updates() {
	s._lock.Lock()
	for _, tx := range s.tasks {
		for _, sub := range tx.subscribers {
			select {
			case sub <- tx.status:
				continue
			default:
				continue
			}
		}
	}
	s._lock.Unlock()
}

func (s *Scheduler) Start(quit chan int) {
	s.timer = time.NewTicker(time.Microsecond * 100)
	for worker, q := range s.workers {
		go worker.work(s.queue, q)
	}
	go func() {
		for {
			select {
			case <-s.timer.C:
				go s.schedule()
				go s.updates()
			case <-quit:
				s.timer.Stop()
				for _, q := range s.workers {
					q <- 0
				}
				return
			}
		}
	}()
}
