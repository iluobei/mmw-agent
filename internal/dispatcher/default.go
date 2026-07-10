package dispatcher

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	officialdispatcher "github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"

	"mmw-agent/internal/limiter"
)

var errSniffingTimeout = errors.New("timeout on sniffing")

type cachedReader struct {
	sync.Mutex
	reader buf.TimeoutReader
	cache  buf.MultiBuffer
}

func (r *cachedReader) Cache(b *buf.Buffer, deadline time.Duration) error {
	mb, err := r.reader.ReadMultiBufferTimeout(deadline)
	if err != nil {
		return err
	}
	r.Lock()
	if !mb.IsEmpty() {
		r.cache, _ = buf.MergeMulti(r.cache, mb)
	}
	b.Clear()
	rawBytes := b.Extend(min(r.cache.Len(), b.Cap()))
	n := r.cache.Copy(rawBytes)
	b.Resize(0, int32(n))
	r.Unlock()
	return nil
}

func (r *cachedReader) readInternal() buf.MultiBuffer {
	r.Lock()
	defer r.Unlock()
	if r.cache != nil && !r.cache.IsEmpty() {
		mb := r.cache
		r.cache = nil
		return mb
	}
	return nil
}

func (r *cachedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if mb := r.readInternal(); mb != nil {
		return mb, nil
	}
	return r.reader.ReadMultiBuffer()
}

func (r *cachedReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	if mb := r.readInternal(); mb != nil {
		return mb, nil
	}
	return r.reader.ReadMultiBufferTimeout(timeout)
}

func (r *cachedReader) Interrupt() {
	r.Lock()
	if r.cache != nil {
		r.cache = buf.ReleaseMulti(r.cache)
	}
	r.Unlock()
	if p, ok := r.reader.(*pipe.Reader); ok {
		p.Interrupt()
	}
}

type Dispatcher struct {
	ohm    outbound.Manager
	router routing.Router
	policy policy.Manager
	stats  stats.Manager
	fdns   dns.FakeDNSEngine

	Limiter *limiter.Limiter
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		d := &Dispatcher{}
		if err := core.RequireFeatures(ctx, func(om outbound.Manager, router routing.Router, pm policy.Manager, sm stats.Manager) error {
			core.OptionalFeatures(ctx, func(fdns dns.FakeDNSEngine) {
				d.fdns = fdns
			})
			d.ohm = om
			d.router = router
			d.policy = pm
			d.stats = sm
			d.Limiter = limiter.New()
			return nil
		}); err != nil {
			return nil, err
		}
		return d, nil
	}))
}

func (*Dispatcher) Type() interface{} { return Type() }
func (*Dispatcher) Start() error      { return nil }
func (*Dispatcher) Close() error      { return nil }

func (d *Dispatcher) getLink(ctx context.Context) (*transport.Link, *transport.Link, error) {
	opt := pipe.OptionsFromContext(ctx)
	uplinkReader, uplinkWriter := pipe.New(opt...)
	downlinkReader, downlinkWriter := pipe.New(opt...)

	inboundLink := &transport.Link{
		Reader: downlinkReader,
		Writer: uplinkWriter,
	}
	outboundLink := &transport.Link{
		Reader: uplinkReader,
		Writer: downlinkWriter,
	}

	sessionInbound := session.InboundFromContext(ctx)
	var user *protocol.MemoryUser
	if sessionInbound != nil {
		sessionInbound.CanSpliceCopy = 3
		user = sessionInbound.User
	}

	if user != nil && len(user.Email) > 0 {
		// 连接数限制:按 group 精确并发计数、满额拒绝。在建出站前断流(零出站占用);
		// 放行则注册 ctx 结束时 ReleaseConn 精确 -1。group="" 表示不限/不计数,无需释放。
		if ok, group := d.Limiter.AcquireConn(sessionInbound.Tag, user.Email); !ok {
			errors.LogWarning(ctx, "connection limit reached: ", user.Email)
			common.Close(outboundLink.Writer)
			common.Close(inboundLink.Writer)
			common.Interrupt(outboundLink.Reader)
			common.Interrupt(inboundLink.Reader)
			return nil, nil, errors.New("connection limit reached: ", user.Email)
		} else if group != "" {
			context.AfterFunc(ctx, func() { d.Limiter.ReleaseConn(group) })
		}

		bucket, hasLimit, _ := d.Limiter.GetUserBucket(
			sessionInbound.Tag,
			user.Email,
			sessionInbound.Source.Address.IP().String(),
		)
		if hasLimit {
			inboundLink.Writer = d.Limiter.RateWriter(inboundLink.Writer, bucket)
			outboundLink.Writer = d.Limiter.RateWriter(outboundLink.Writer, bucket)
		}

		p := d.policy.ForLevel(user.Level)
		if p.Stats.UserUplink {
			name := "user>>>" + user.Email + ">>>traffic>>>uplink"
			if c, _ := stats.GetOrRegisterCounter(d.stats, name); c != nil {
				inboundLink.Writer = &officialdispatcher.SizeStatWriter{
					Counter: c,
					Writer:  inboundLink.Writer,
				}
			}
		}
		if p.Stats.UserDownlink {
			name := "user>>>" + user.Email + ">>>traffic>>>downlink"
			if c, _ := stats.GetOrRegisterCounter(d.stats, name); c != nil {
				outboundLink.Writer = &officialdispatcher.SizeStatWriter{
					Counter: c,
					Writer:  outboundLink.Writer,
				}
			}
		}

		if p.Stats.UserOnline {
			name := "user>>>" + user.Email + ">>>online"
			if om, _ := stats.GetOrRegisterOnlineMap(d.stats, name); om != nil {
				userIP := sessionInbound.Source.Address.String()
				om.AddIP(userIP)
				context.AfterFunc(ctx, func() { om.RemoveIP(userIP) })
			}
		}
	}

	return inboundLink, outboundLink, nil
}

func (d *Dispatcher) shouldOverride(ctx context.Context, result officialdispatcher.SniffResult, request session.SniffingRequest, destination net.Destination) bool {
	domain := result.Domain()
	if domain == "" {
		return false
	}
	for _, dd := range request.ExcludeForDomain {
		if strings.HasPrefix(dd, "regexp:") {
			re, err := regexp.Compile(dd[7:])
			if err != nil {
				continue
			}
			if re.MatchString(domain) {
				return false
			}
		} else if strings.ToLower(domain) == dd {
			return false
		}
	}
	protocolString := result.Protocol()
	if resComp, ok := result.(officialdispatcher.SnifferResultComposite); ok {
		protocolString = resComp.ProtocolForDomainResult()
	}
	for _, p := range request.OverrideDestinationForProtocol {
		if strings.HasPrefix(protocolString, p) || strings.HasPrefix(p, protocolString) {
			return true
		}
		if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && protocolString != "bittorrent" && p == "fakedns" &&
			destination.Address.Family().IsIP() && fkr0.IsIPInIPPool(destination.Address) {
			return true
		}
		if resultSubset, ok := result.(officialdispatcher.SnifferIsProtoSubsetOf); ok {
			if resultSubset.IsProtoSubsetOf(p) {
				return true
			}
		}
	}
	return false
}

func (d *Dispatcher) Dispatch(ctx context.Context, destination net.Destination) (*transport.Link, error) {
	if !destination.IsValid() {
		panic("Dispatcher: Invalid destination.")
	}
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		outbounds = []*session.Outbound{{}}
		ctx = session.ContextWithOutbounds(ctx, outbounds)
	}
	ob := outbounds[len(outbounds)-1]
	ob.OriginalTarget = destination
	ob.Target = destination
	content := session.ContentFromContext(ctx)
	if content == nil {
		content = new(session.Content)
		ctx = session.ContextWithContent(ctx, content)
	}

	sniffingRequest := content.SniffingRequest
	inbound, outbound, err := d.getLink(ctx)
	if err != nil {
		return nil, err
	}
	if !sniffingRequest.Enabled {
		go d.routedDispatch(ctx, outbound, destination)
	} else {
		go func() {
			cReader := &cachedReader{
				reader: outbound.Reader.(*pipe.Reader),
			}
			outbound.Reader = cReader
			result, err := d.sniffer(ctx, cReader, sniffingRequest.MetadataOnly, destination.Network)
			if err == nil {
				content.Protocol = result.Protocol()
			}
			if err == nil && d.shouldOverride(ctx, result, sniffingRequest, destination) {
				domain := result.Domain()
				errors.LogInfo(ctx, "sniffed domain: ", domain)
				destination.Address = net.ParseAddress(domain)
				protocolStr := result.Protocol()
				if resComp, ok := result.(officialdispatcher.SnifferResultComposite); ok {
					protocolStr = resComp.ProtocolForDomainResult()
				}
				isFakeIP := false
				if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && fkr0.IsIPInIPPool(ob.Target.Address) {
					isFakeIP = true
				}
				if sniffingRequest.RouteOnly && protocolStr != "fakedns" && protocolStr != "fakedns+others" && !isFakeIP {
					ob.RouteTarget = destination
				} else {
					ob.Target = destination
				}
			}
			d.routedDispatch(ctx, outbound, destination)
		}()
	}
	return inbound, nil
}

func (d *Dispatcher) DispatchLink(ctx context.Context, destination net.Destination, outbound *transport.Link) error {
	if !destination.IsValid() {
		return errors.New("Dispatcher: Invalid destination.")
	}
	// 禁用 vision splice 优化的 OS 级 splice syscall (CanSpliceCopy=1 那条快路径)。
	// vless inbound (XRV flow) 进入 dispatcher 前已经把 inbound.CanSpliceCopy 设为 2;
	// 不在这里改为 3,vision 协议在 WriteMultiBuffer 走 line 339-340 会保留 spliceReadyInbound,
	// 然后写完 31 字节切换指令后立刻把 CanSpliceCopy 升级为 1 触发 kernel splice,**完全绕过 RateLimitedConn**。
	// 强制设为 3 (disable splice) 让数据全程走 user-space buf.Writer/conn.Write,vision_limiter_hook
	// 包的 wrap conn 才能拦截读写。详见 XrayR PR #757 + xray-core issue #3100。
	si := session.InboundFromContext(ctx)
	if si != nil {
		si.CanSpliceCopy = 3
		// 连接数限制:DispatchLink 是 VLESS 等的真实数据路径(不经 getLink),必须在这里也做满额拒绝,
		// 否则限制形同虚设(历史 device_limit 同样只挂在 getLink,对 DispatchLink 路径无效)。
		if si.User != nil && len(si.User.Email) > 0 {
			if ok, group := d.Limiter.AcquireConn(si.Tag, si.User.Email); !ok {
				errors.LogWarning(ctx, "connection limit reached: ", si.User.Email)
				common.Interrupt(outbound.Reader)
				common.Close(outbound.Writer)
				return errors.New("connection limit reached: ", si.User.Email)
			} else if group != "" {
				context.AfterFunc(ctx, func() { d.Limiter.ReleaseConn(group) })
			}
		}
	}
	outbounds := session.OutboundsFromContext(ctx)
	if len(outbounds) == 0 {
		outbounds = []*session.Outbound{{}}
		ctx = session.ContextWithOutbounds(ctx, outbounds)
	}
	ob := outbounds[len(outbounds)-1]
	ob.OriginalTarget = destination
	ob.Target = destination
	content := session.ContentFromContext(ctx)
	if content == nil {
		content = new(session.Content)
		ctx = session.ContextWithContent(ctx, content)
	}
	outbound = officialdispatcher.WrapLink(ctx, d.policy, d.stats, outbound)
	sniffingRequest := content.SniffingRequest
	if !sniffingRequest.Enabled {
		d.routedDispatch(ctx, outbound, destination)
	} else {
		cReader := &cachedReader{
			reader: outbound.Reader.(buf.TimeoutReader),
		}
		outbound.Reader = cReader
		result, err := d.sniffer(ctx, cReader, sniffingRequest.MetadataOnly, destination.Network)
		if err == nil {
			content.Protocol = result.Protocol()
		}
		if err == nil && d.shouldOverride(ctx, result, sniffingRequest, destination) {
			domain := result.Domain()
			errors.LogInfo(ctx, "sniffed domain: ", domain)
			destination.Address = net.ParseAddress(domain)
			protocolStr := result.Protocol()
			if resComp, ok := result.(officialdispatcher.SnifferResultComposite); ok {
				protocolStr = resComp.ProtocolForDomainResult()
			}
			isFakeIP := false
			if fkr0, ok := d.fdns.(dns.FakeDNSEngineRev0); ok && fkr0.IsIPInIPPool(ob.Target.Address) {
				isFakeIP = true
			}
			if sniffingRequest.RouteOnly && protocolStr != "fakedns" && protocolStr != "fakedns+others" && !isFakeIP {
				ob.RouteTarget = destination
			} else {
				ob.Target = destination
			}
		}
		d.routedDispatch(ctx, outbound, destination)
	}
	return nil
}

func (d *Dispatcher) sniffer(ctx context.Context, cReader *cachedReader, metadataOnly bool, network net.Network) (officialdispatcher.SniffResult, error) {
	payload := buf.NewWithSize(32767)
	defer payload.Release()

	sniffer := officialdispatcher.NewSniffer(ctx)
	metaresult, metadataErr := sniffer.SniffMetadata(ctx)

	if metadataOnly {
		return metaresult, metadataErr
	}

	contentResult, contentErr := func() (officialdispatcher.SniffResult, error) {
		cacheDeadline := 200 * time.Millisecond
		totalAttempt := 0
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				start := time.Now()
				err := cReader.Cache(payload, cacheDeadline)
				if err != nil {
					return nil, err
				}
				cacheDeadline -= time.Since(start)

				if !payload.IsEmpty() {
					result, err := sniffer.Sniff(ctx, payload.Bytes(), network)
					switch err {
					case common.ErrNoClue:
						totalAttempt++
					case protocol.ErrProtoNeedMoreData:
						// allow to read until timeout
					default:
						return result, err
					}
				} else {
					totalAttempt++
				}
				if totalAttempt >= 2 || cacheDeadline <= 0 {
					return nil, errSniffingTimeout
				}
			}
		}
	}()
	if contentErr != nil && metadataErr == nil {
		return metaresult, nil
	}
	if contentErr == nil && metadataErr == nil {
		return officialdispatcher.CompositeResult(metaresult, contentResult), nil
	}
	return contentResult, contentErr
}

func (d *Dispatcher) routedDispatch(ctx context.Context, link *transport.Link, destination net.Destination) {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]

	var handler outbound.Handler

	routingLink := routing_session.AsRoutingContext(ctx)
	inTag := routingLink.GetInboundTag()
	isPickRoute := 0
	if forcedOutboundTag := session.GetForcedOutboundTagFromContext(ctx); forcedOutboundTag != "" {
		ctx = session.SetForcedOutboundTagToContext(ctx, "")
		if h := d.ohm.GetHandler(forcedOutboundTag); h != nil {
			isPickRoute = 1
			errors.LogInfo(ctx, "taking platform initialized detour [", forcedOutboundTag, "] for [", destination, "]")
			handler = h
		} else {
			errors.LogError(ctx, "non existing tag for platform initialized detour: ", forcedOutboundTag)
			common.Close(link.Writer)
			common.Interrupt(link.Reader)
			return
		}
	} else if d.router != nil {
		if route, err := d.router.PickRoute(routingLink); err == nil {
			outTag := route.GetOutboundTag()
			if h := d.ohm.GetHandler(outTag); h != nil {
				isPickRoute = 2
				if route.GetRuleTag() == "" {
					errors.LogInfo(ctx, "taking detour [", outTag, "] for [", destination, "]")
				} else {
					errors.LogInfo(ctx, "Hit route rule: [", route.GetRuleTag(), "] so taking detour [", outTag, "] for [", destination, "]")
				}
				handler = h
			} else {
				errors.LogWarning(ctx, "non existing outTag: ", outTag)
				common.Close(link.Writer)
				common.Interrupt(link.Reader)
				return
			}
		} else {
			errors.LogInfo(ctx, "default route for ", destination)
		}
	}

	if handler == nil {
		handler = d.ohm.GetDefaultHandler()
	}
	if handler == nil {
		errors.LogInfo(ctx, "default outbound handler not exist")
		common.Close(link.Writer)
		common.Interrupt(link.Reader)
		return
	}

	ob.Tag = handler.Tag()
	if accessMessage := log.AccessMessageFromContext(ctx); accessMessage != nil {
		if tag := handler.Tag(); tag != "" {
			if inTag == "" {
				accessMessage.Detour = tag
			} else if isPickRoute == 1 {
				accessMessage.Detour = inTag + " ==> " + tag
			} else if isPickRoute == 2 {
				accessMessage.Detour = inTag + " -> " + tag
			} else {
				accessMessage.Detour = inTag + " >> " + tag
			}
		}
		log.Record(accessMessage)
	}

	handler.Dispatch(ctx, link)
}
