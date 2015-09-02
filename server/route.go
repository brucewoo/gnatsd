// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"time"
	"strings"
)

type route struct {
	remoteID   string
	url        *url.URL
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	Ssl      bool   `json:"ssl_required"`
	Name     string `json:"name"`
}

const conProto = "CONNECT %s" + _CRLF_

// Lock should be held entering here.
func (c *client) sendConnect() {
	var user, pass string
	if userInfo := c.route.url.User; userInfo != nil {
		user = userInfo.Username()
		pass, _ = userInfo.Password()
	}
	cinfo := connectInfo{
		Verbose:  false,
		Pedantic: false,
		User:     user,
		Pass:     pass,
		Ssl:      false,
		Name:     c.srv.info.ID,
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		Logf("Error marshalling CONNECT to route: %v\n", err)
		c.closeConnection()
	}
	c.bw.WriteString(fmt.Sprintf(conProto, b))
	c.bw.Flush()
}

// This will send local subscription state to a new route connection.
// FIXME(dlc) - This could be a DOS or perf issue with many clients
// and large subscription space. Plus buffering in place not a good idea.
func (s *Server) sendLocalSubsToRoute(route *client) {
	b := bytes.Buffer{}
	hasSentSubs := make(map[string]bool)

	s.mu.Lock()
	for _, client := range s.clients {
		client.mu.Lock()
		subs := client.subs.All()
		client.mu.Unlock()
		for _, s := range subs {
			if sub, ok := s.(*subscription); ok && sub.client.typ != ROUTER {
				//如果一个主题已经被发送过了就不需要在发送了，减少订阅信息
				_, exist := hasSentSubs[string(sub.subject)]
				if !exist {
					hasSentSubs[string(sub.subject)] = true
					rsid := routeSid(sub)
					proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
					b.WriteString(proto)
					Logf("%s  send to other route success.", string(sub.subject))
				} else {
					Logf("%s has sent to other route.", string(sub.subject))
				}
			}
		}
	}
	s.mu.Unlock()

	route.mu.Lock()
	ip, port := route.ConnStr()
	_, err := route.bw.Write(b.Bytes())
	if err != nil {
		route.mu.Unlock()
		Logf("Write:Route sent local subscriptions to connect route[%s:%s/%d] failed.", ip, port, route.cid)
		route.closeConnection()
		return 
	}
	err = route.bw.Flush()
	if err != nil {
		route.mu.Unlock()
		Logf("Flush:Route sent local subscriptions to connect route[%s:%s/%d] failed.", ip, port, route.cid)
		route.closeConnection()
		return
	} 
	route.mu.Unlock()
	Logf("Route sent local subscriptions to connect route[%s:%s/%d] success.", ip, port, route.cid)
}

func (s *Server) createRoute(conn net.Conn, rURL *url.URL) *client {
	r := &route{}
	c := &client{srv: s, nc: conn, opts: clientOpts{}, typ: ROUTER, route: r}

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient()

	Log("Route connection created ", clientConnStr(c.nc), c.cid)

	// Queue Connect proto if we solicited the connection.
	if rURL != nil {
		r.url = rURL
		Debug("Route connect msg sent", clientConnStr(c.nc), c.cid)
		c.sendConnect()
	}

	// Send our info to the other side.
	s.sendInfo(c)

	// Check for Auth required state for incoming connections.
	if s.routeInfo.AuthRequired {
		ttl := secondsToDuration(s.opts.AuthTimeout)
		c.setAuthTimer(ttl)
	}

	// Unlock to register.
	c.mu.Unlock()

	// Register with the server.
	s.mu.Lock()
	s.routes[c.cid] = c
	s.mu.Unlock()

	// Send our local subscriptions to this route.
	s.sendLocalSubsToRoute(c)

	return c
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
	_SPC_   = " "
)

const (
	subProto   = "SUB %s %s %s" + _CRLF_
	unsubProto = "UNSUB %s%s" + _CRLF_
)

// FIXME(dlc) - Make these reserved and reject if they come in as a sid
// from a client connection.

const (
	RSID  = "RSID"
	QRSID = "QRSID"

	RSID_CID_INDEX   = 1
	RSID_SID_INDEX   = 2
	EXPECTED_MATCHES = 3
)

// FIXME(dlc) - This may be too slow, check at later date.
var qrsidRe = regexp.MustCompile(`QRSID:(\d+):([^\s]+)`)

func (s *Server) routeSidQueueSubscriber(rsid []byte) (*subscription, bool){
	if !bytes.HasPrefix(rsid, []byte(QRSID)) {
		return nil, false
	}
	matches := qrsidRe.FindSubmatch(rsid)
	if matches == nil || len(matches) != EXPECTED_MATCHES {
		return nil, false
	}
	cid := uint64(parseInt64(matches[RSID_CID_INDEX]))
	client := s.clients[cid]
	if client == nil {
		return nil, true
	}
	sid := matches[RSID_SID_INDEX]
	if sub, ok := (client.subs.Get(sid)).(*subscription); ok {
		return sub, true
	}
	return nil, true
}

func routeSid(sub *subscription) string {
	var qi string
	if len(sub.queue) > 0 {
		qi = "Q"
	}
	return fmt.Sprintf("%s%s:%d:%s", qi, RSID, sub.client.cid, sub.sid)
}

func (s *Server) isDuplicateRemote(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.remotes[id]
	return ok
}

func (s *Server) broadcastToRoutes(proto string) error {
	var err error
	
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, route := range s.routes {
		// FIXME(dlc) - Make same logic as deliverMsg
		ip, port := route.ConnStr()
		if len(ip) < 8 {
			continue
		}
		route.mu.Lock()
		_, err = route.bw.WriteString(proto)
		if err != nil {
			Logf("broadcast to route subscriptions to [%s:%s/%d] failed, WriteString failed.", ip, port, route.cid)
		} else {
			err = route.bw.Flush()
			if err != nil {
				Logf("broadcast to route subscriptions to [%s:%s/%d] failed, WriteString failed.", ip, port, route.cid)
			}
		}
		route.mu.Unlock()
		
		if err != nil {
			break
		} else {
			Logf("broadcast to route subscriptions to [%s:%s/%d][%s] success.", ip, port, route.cid, proto)
		}
	}
	return err
}

// broadcastSubscribe will forward a client subscription
// to all active routes.
func (s *Server) broadcastSubscribe(sub *subscription) error {
	rsid := routeSid(sub)
	proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
	return s.broadcastToRoutes(proto)
}

// broadcastUnSubscribe will forward a client unsubscribe
// action to all active routes.
func (s *Server) broadcastUnSubscribe(sub *subscription) error {
	rsid := routeSid(sub)
	maxStr := _EMPTY_
	if sub.max > 0 {
		maxStr = fmt.Sprintf("%d ", sub.max)
	}
	proto := fmt.Sprintf(unsubProto, maxStr, rsid)
	return s.broadcastToRoutes(proto)
}

func (s *Server) routeAcceptLoop(ch chan struct{}) {
	hp := fmt.Sprintf("%s:%d", s.opts.RouteHost, s.opts.RoutePort)
	Logf("Listening for route connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on router port: %d - %v", s.opts.Port, e)
		return
	}

	// Let them know we are up
	close(ch)

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.routeListener = l
	s.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Debug("Temporary Route Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if s.isRunning() {
				Logf("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		
		routeURL := "nats-route://" + s.opts.Username + ":" + s.opts.Password + "@"
		if ip, ok := conn.(*net.TCPConn); ok {
			addr := ip.RemoteAddr().(*net.TCPAddr)
			routeURL += addr.String()
			Logf("%s", routeURL)
			rUrl, err := url.Parse(routeURL)
			if err != nil {
	  			Logf("error parsing route url [%q]", routeURL)
	  			continue
	  		}
			s.createRoute(conn, rUrl)
		}
	}
	Debug("Router accept loop exiting..")
	s.done <- true
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting() {
	info := Info{
		ID:           s.info.ID,
		Version:      s.info.Version,
		Host:         s.opts.RouteHost,
		Port:         s.opts.RoutePort,
		AuthRequired: false,
		SslRequired:  false,
		MaxPayload:   MAX_PAYLOAD_SIZE,
	}
	// Check for Auth items
	if s.opts.Username != "" {
		info.AuthRequired = true
	}
	s.routeInfo = info
	// Generate the info json
	b, err := json.Marshal(info)
	if err != nil {
		Fatalf("Error marshalling Route INFO JSON: %+v\n", err)
	}
	s.routeInfoJSON = []byte(fmt.Sprintf("INFO %s %s", b, CR_LF))

	// Spin up the accept loop
	ch := make(chan struct{})
	go s.routeAcceptLoop(ch)
	<-ch
}

func (s *Server) connectToRoute(nodepath string) {
	tmps := strings.Split(nodepath, "-")
	if len(tmps) < 3 {
		return
	}
	routeURL := "nats-route://" + tmps[0] + ":" + tmps[2]
	rUrl, err := url.Parse(routeURL)
	if err != nil {
  		Logf("error parsing route url [%q]", routeURL)
  		return
    }
	
	for s.isRunning() {
		b, _,  err := s.zkClient.Exists(s.opts.ZkPath + "/" + nodepath)
		if err != nil {
			Logf("zkclient Exists [%s] failed.", nodepath)
			continue
		}
		
		if !b {
			Logf("[%s] is not Exists, exit connectToRoute.", nodepath)
			return
		}
		
		Logf("Trying to connect to route on %s", rUrl.Host)
		conn, err := net.DialTimeout("tcp", rUrl.Host, DEFAULT_ROUTE_DIAL)
		if err != nil {
			Debugf("Error trying to connect to route: %v", err)
			select {
			case <-s.rcQuit:
				return
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				continue
			}
		}
		// We have a route connection here.
		// Go ahead and create it and exit this func.
		if ip, ok := conn.(*net.TCPConn); ok {
			routeURL = "nats-route://" + s.opts.Username + ":" + s.opts.Password + "@"
			addr := ip.RemoteAddr().(*net.TCPAddr)
			routeURL += addr.String()
			Logf("%s", routeURL)
			rUrl, err = url.Parse(routeURL)
			if err != nil {
	  			Logf("error parsing route url [%q]", routeURL)
	  		} else {
				s.createRoute(conn, rUrl)
			}
		}
		return
	}
}
