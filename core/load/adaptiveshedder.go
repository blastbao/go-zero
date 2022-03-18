package load

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultBuckets = 50
	defaultWindow  = time.Second * 5
	// using 1000m notation, 900m is like 80%, keep it as var for unit test
	defaultCpuThreshold = 900
	defaultMinRt        = float64(time.Second / time.Millisecond)
	// moving average hyperparameter beta for calculating requests on the fly
	flyingBeta      = 0.9
	coolOffDuration = time.Second
)

var (
	// ErrServiceOverloaded is returned by Shedder.Allow when the service is overloaded.
	ErrServiceOverloaded = errors.New("service overloaded")

	// default to be enabled
	enabled = syncx.ForAtomicBool(true)
	// default to be enabled
	logEnabled = syncx.ForAtomicBool(true)

	// make it a variable for unit test
	//
	// cpu 检查函数
	systemOverloadChecker = func(cpuThreshold int64) bool {
		return stat.CpuUsage() >= cpuThreshold
	}
)

type (
	// A Promise interface is returned by Shedder.Allow to let callers tell
	// whether the processing request is successful or not.
	Promise interface {
		// Pass lets the caller tell that the call is successful.
		Pass()
		// Fail lets the caller tell that the call is failed.
		Fail()
	}

	// Shedder is the interface that wraps the Allow method.
	Shedder interface {
		// Allow returns the Promise if allowed, otherwise ErrServiceOverloaded.
		Allow() (Promise, error)
	}

	// ShedderOption lets caller customize the Shedder.
	// option 参数
	ShedderOption func(opts *shedderOptions)

	// 可选配置参数
	shedderOptions struct {
		// 窗口大小
		window time.Duration
		// 窗口数量
		buckets int
		// cpu 负载临界值
		cpuThreshold int64
	}

	// 自适应降载结构体，需实现 Shedder 接口
	adaptiveShedder struct {
		// cpu 负载临界值，高于临界值代表高负载需要降载保证服务
		cpuThreshold int64
		// 1s 内有多少个桶
		windows int64
		// 并发数
		flying int64
		// 滑动平滑并发数
		avgFlying float64
		// 自旋锁，一个服务共用一个降载
		// 统计当前正在处理的请求数时必须加锁
		// 无损并发，提高性能
		avgFlyingLock syncx.SpinLock
		// 最后一次拒绝时间
		dropTime *syncx.AtomicDuration
		// 最近是否被拒绝过
		droppedRecently *syncx.AtomicBool
		// 请求数统计，通过滑动时间窗口记录最近一段时间内指标
		passCounter *collection.RollingWindow
		// 响应时间统计，通过滑动时间窗口记录最近一段时间内指标
		rtCounter *collection.RollingWindow
	}
)

// Disable lets callers disable load shedding.
func Disable() {
	enabled.Set(false)
}

// DisableLog disables the stat logs for load shedding.
func DisableLog() {
	logEnabled.Set(false)
}

// NewAdaptiveShedder returns an adaptive shedder.
// opts can be used to customize the Shedder.
//
// 自适应降载
func NewAdaptiveShedder(opts ...ShedderOption) Shedder {

	// 当开发者关闭时返回默认的空实现
	if !enabled.True() {
		return newNopShedder()
	}

	// 默认配置
	options := shedderOptions{
		window:       defaultWindow,
		buckets:      defaultBuckets,
		cpuThreshold: defaultCpuThreshold,
	}

	// 人为配置
	for _, opt := range opts {
		opt(&options)
	}

	// 计算每个窗口间隔时间，默认为100ms
	bucketDuration := options.window / time.Duration(options.buckets)
	return &adaptiveShedder{
		// cpu负载
		cpuThreshold:    options.cpuThreshold,
		// 1s的时间内包含多少个滑动窗口单元
		windows:         int64(time.Second / bucketDuration),
		// 最近一次拒绝时间
		dropTime:        syncx.NewAtomicDuration(),
		// 最近是否被拒绝过
		droppedRecently: syncx.NewAtomicBool(),
		// qps统计，滑动时间窗口
		// 忽略当前正在写入窗口（桶），时间周期不完整可能导致数据异常
		passCounter: collection.NewRollingWindow(options.buckets, bucketDuration, collection.IgnoreCurrentBucket()),
		// 响应时间统计，滑动时间窗口
		// 忽略当前正在写入窗口（桶），时间周期不完整可能导致数据异常
		rtCounter: collection.NewRollingWindow(options.buckets, bucketDuration, collection.IgnoreCurrentBucket()),
	}
}

// Allow implements Shedder.Allow.
//
// 降载检查
// 检查当前请求是否应该被丢弃，被丢弃业务侧需要直接中断请求保护服务，也意味着降载生效同时进入冷却期。如果放行则返回 promise，等待业务侧执行回调函数执行指标统计。
func (as *adaptiveShedder) Allow() (Promise, error) {

	// 检查请求是否被丢弃
	if as.shouldDrop() {
		// 设置drop时间
		as.dropTime.Set(timex.Now())
		// 最近已被drop
		as.droppedRecently.Set(true)
		// 返回过载
		return nil, ErrServiceOverloaded
	}

	// 正在处理请求数加1
	as.addFlying(1)

	// 这里每个允许的请求都会返回一个新的 promise 对象，promise 内部持有了降载对象指针
	return &promise{
		start:   timex.Now(),
		shedder: as,
	}, nil
}


// 如何得到正在处理的并发数与平均并发数呢？
// 当前正在的处理并发数统计其实非常简单，每次允许请求时并发数 +1，请求完成后通过 promise 对象回调-1 即可，
// 并利用滑动平均算法求解平均并发数即可。
func (as *adaptiveShedder) addFlying(delta int64) {
	flying := atomic.AddInt64(&as.flying, delta)
	// update avgFlying when the request is finished.
	// this strategy makes avgFlying have a little bit lag against flying, and smoother.
	// when the flying requests increase rapidly, avgFlying increase slower, accept more requests.
	// when the flying requests drop rapidly, avgFlying drop slower, accept less requests.
	// it makes the service to serve as more requests as possible.
	//
	// 请求结束后，统计当前正在处理的请求并发
	if delta < 0 {
		as.avgFlyingLock.Lock()
		// 估算当前服务近一段时间内的平均请求数
		as.avgFlying = as.avgFlying*flyingBeta + float64(flying)*(1-flyingBeta)
		as.avgFlyingLock.Unlock()
	}
}


// 检查当前正在处理的并发数
//
// 一旦 当前处理的并发数 > 并发数承载上限 则进入降载状态。
//
// 这里为什么要加锁呢？
// 因为自适应降载时全局在使用的，为了保证并发数平均值正确性。
//
// 为什么这里要加自旋锁呢？
// 因为并发处理过程中，可以不阻塞其他的 goroutine 执行任务，采用无锁并发提高性能。
//
func (as *adaptiveShedder) highThru() bool {
	as.avgFlyingLock.Lock()
	// 获取滑动平均值，每次请求结束后更新
	avgFlying := as.avgFlying
	as.avgFlyingLock.Unlock()
	// 系统此时最大并发数
	maxFlight := as.maxFlight()
	// 正在处理的并发数和平均并发数是否大于系统的最大并发数
	return int64(avgFlying) > maxFlight && atomic.LoadInt64(&as.flying) > maxFlight
}

// 得到了当前的系统数还不够 ，我们还需要知道当前系统能够处理并发数的上限，即最大并发数。
//
// 请求通过数与响应时间都是通过滑动窗口来实现的，关于滑动窗口的实现可以参考 "自适应熔断器" 那篇文章。
//
// 当前系统的最大并发数 = 窗口单位时间内的最大通过数量 * 窗口单位时间内的最小响应时间。
//
// 即：每秒最大并发数 = 最大请求数（qps）* 最小响应时间（rt）
func (as *adaptiveShedder) maxFlight() int64 {
	// windows = buckets per second
	// maxQPS = maxPASS * windows
	// minRT = min average response time in milliseconds
	// maxQPS * minRT / milliseconds_per_second
	//
	// as.maxPass()*as.windows ---- 每个桶最大的qps * 1s内包含桶的数量
	// as.minRt()/1e3 ---- 窗口所有桶中最小的平均响应时间 / 1000ms 这里是为了转换成秒
	return int64(math.Max(1, float64(as.maxPass()*as.windows)*(as.minRt()/1e3)))
}

// 滑动时间窗口内有多个桶
// 找到请求数最多的那个
// 每个桶占用的时间为 internal ms
// qps指的是1s内的请求数，qps: maxPass * time.Second/internal
func (as *adaptiveShedder) maxPass() int64 {
	var result float64 = 1

	// 当前时间窗口内请求数最多的桶
	as.passCounter.Reduce(func(b *collection.Bucket) {
		if b.Sum > result {
			result = b.Sum
		}
	})

	return int64(result)
}


// 滑动时间窗口内有多个桶
// 计算最小的平均响应时间
// 因为需要计算近一段时间内系统能够处理的最大并发数
func (as *adaptiveShedder) minRt() float64 {

	// 默认为1000ms
	result := defaultMinRt

	as.rtCounter.Reduce(func(b *collection.Bucket) {
		if b.Count <= 0 {
			return
		}

		// 请求平均响应时间
		avg := math.Round(b.Sum / float64(b.Count))
		if avg < result {
			result = avg
		}
	})

	return result
}


// 检查请求是否应该被丢弃
func (as *adaptiveShedder) shouldDrop() bool {
	// 当前 cpu 负载超过阈值
	// 服务处于冷却期内应该继续检查负载并尝试丢弃请求
	if as.systemOverloaded() || as.stillHot() {
		// 检查正在处理的并发是否超出当前可承载的最大并发数，超出则丢弃请求
		if as.highThru() {
			flying := atomic.LoadInt64(&as.flying)
			as.avgFlyingLock.Lock()
			avgFlying := as.avgFlying
			as.avgFlyingLock.Unlock()
			msg := fmt.Sprintf(
				"dropreq, cpu: %d, maxPass: %d, minRt: %.2f, hot: %t, flying: %d, avgFlying: %.2f",
				stat.CpuUsage(), as.maxPass(), as.minRt(), as.stillHot(), flying, avgFlying)
			logx.Error(msg)
			stat.Report(msg)
			return true
		}
	}

	return false
}


// 判断当前系统是否处于冷却期
// 如果处于冷却期内，应该继续尝试检查是否丢弃请求。
// 主要是防止系统在过载恢复过程中负载还未降下来，立马又增加压力导致来回抖动，此时应该尝试继续丢弃请求。
func (as *adaptiveShedder) stillHot() bool {

	// 最近没有丢弃请求
	// 说明服务正常
	if !as.droppedRecently.True() {
		return false
	}

	// 不在冷却期
	dropTime := as.dropTime.Load()
	if dropTime == 0 {
		return false
	}

	// 冷却时间默认为 1s
	hot := timex.Since(dropTime) < coolOffDuration

	// 不在冷却期，正常处理请求中
	if !hot {
		// 重置drop记录
		as.droppedRecently.Set(false)
	}

	return hot
}


// cpu 阈值检查
//
// cpu 负载值计算算法采用的滑动平均算法，防止毛刺现象。
// 每隔 250ms 采样一次 β 为 0.95，大概相当于历史 20 次 cpu 负载的平均值，时间周期约为 5s。
func (as *adaptiveShedder) systemOverloaded() bool {
	return systemOverloadChecker(as.cpuThreshold)
}

// WithBuckets customizes the Shedder with given number of buckets.
func WithBuckets(buckets int) ShedderOption {
	return func(opts *shedderOptions) {
		opts.buckets = buckets
	}
}

// WithCpuThreshold customizes the Shedder with given cpu threshold.
func WithCpuThreshold(threshold int64) ShedderOption {
	return func(opts *shedderOptions) {
		opts.cpuThreshold = threshold
	}
}

// WithWindow customizes the Shedder with given
func WithWindow(window time.Duration) ShedderOption {
	return func(opts *shedderOptions) {
		opts.window = window
	}
}

type promise struct {
	start   time.Duration
	shedder *adaptiveShedder
}

func (p *promise) Fail() {
	p.shedder.addFlying(-1)
}

func (p *promise) Pass() {
	rt := float64(timex.Since(p.start)) / float64(time.Millisecond)
	p.shedder.addFlying(-1)
	p.shedder.rtCounter.Add(math.Ceil(rt))
	p.shedder.passCounter.Add(1)
}
