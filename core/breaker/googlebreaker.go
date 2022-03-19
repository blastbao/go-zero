package breaker

import (
	"math"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	window     = time.Second * 10	// 10s
	buckets    = 40					// 40 个桶
	k          = 1.5
	protection = 5
)

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	k     float64
	stat  *collection.RollingWindow
	proba *mathx.Proba
}

func newGoogleBreaker() *googleBreaker {

	// 把 10 秒均分为 40 个桶，每个 400ms
	bucketDuration := time.Duration(int64(window) / int64(buckets))

	// 根据 桶总数(40) 和 每个桶大小(10s) 创建滑动窗口
	st := collection.NewRollingWindow(buckets, bucketDuration)

	// 初始化
	return &googleBreaker{
		stat:  st,
		k:     k,
		proba: mathx.NewProba(),	// 概率生成器
	}
}

func (b *googleBreaker) accept() error {
	// 成功数、总数
	accepts, total := b.history()

	// 成功数 * weight
	weightedAccepts := b.k * float64(accepts)

	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	//
	// 失败率
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))

	// 没有失败，则返回 accept
	if dropRatio <= 0 {
		return nil
	}

	// 如果失败率为 5% ，那么就丢弃 5% 的请求(随机)，其余请求是接受的。
	if b.proba.TrueOnProba(dropRatio) {
		return ErrServiceUnavailable
	}

	return nil
}

func (b *googleBreaker) allow() (internalPromise, error) {
	if err := b.accept(); err != nil {
		return nil, err
	}

	return googlePromise{
		b: b,
	}, nil
}

func (b *googleBreaker) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	if err := b.accept(); err != nil {
		if fallback != nil {
			return fallback(err)
		}

		return err
	}

	defer func() {
		if e := recover(); e != nil {
			b.markFailure()
			panic(e)
		}
	}()

	err := req()
	if acceptable(err) {
		b.markSuccess()
	} else {
		b.markFailure()
	}

	return err
}

func (b *googleBreaker) markSuccess() {
	b.stat.Add(1)
}

func (b *googleBreaker) markFailure() {
	b.stat.Add(0)
}

func (b *googleBreaker) history() (accepts, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})

	return
}

type googlePromise struct {
	b *googleBreaker
}

func (p googlePromise) Accept() {
	p.b.markSuccess()
}

func (p googlePromise) Reject() {
	p.b.markFailure()
}
