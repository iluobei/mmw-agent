package dispatcher

import "github.com/xtls/xray-core/features/routing"

// Type 返回 routing.Dispatcher 这个上游 feature 的类型 token,而不是自定义类型本身。
// 这样 RequireFeatures 能把本地 *Dispatcher 当作"标准 routing.Dispatcher" feature 解析,
// xray-core 内部所有走 routing.Dispatcher 的流量都会进入这个自定义实现 ——
// limiter / per-user RateWriter / user-traffic stats counter 才能挂得上。
//
// 若返回 (*Dispatcher)(nil),自定义 dispatcher 会被注册成另一个独立 feature,
// 实际流量仍走 officialdispatcher,limiter 等钩子完全失效。
func Type() interface{} {
	return routing.DispatcherType()
}
