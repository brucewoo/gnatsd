// Copyright 2012-2013 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"runtime"
	"strings"
	"testing"
	"time"
	"net/url"

	"github.com/apcera/gnatsd/server"
	"github.com/apcera/gnatsd/zk"
)

func runRouteServer(t *testing.T) (*server.Server, *server.Options) {
	opts, err := server.ProcessConfigFile("./configs/cluster.conf")

	// Override for running in Go routine.
	opts.NoSigs = true
	opts.Debug = true
	opts.Trace = true
	opts.NoLog = true

	if err != nil {
		t.Fatalf("Error parsing config file: %v\n", err)
	}
	return RunServer(opts), opts
}

func TestRouterListeningSocket(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	// Check that the cluster socket is able to be connected.
	addr := fmt.Sprintf("%s:%d", opts.RouteHost, opts.RoutePort)
	checkSocket(t, addr, 2*time.Second)
}

func TestRouteGoServerShutdown(t *testing.T) {
	base := runtime.NumGoroutine()
	s, _ := runRouteServer(t)
	s.Shutdown()
	time.Sleep(10 * time.Millisecond)
	delta := (runtime.NumGoroutine() - base)
	if delta > 1 {
		t.Fatalf("%d Go routines still exist post Shutdown()", delta)
	}
}

func TestSendRouteInfoOnConnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()
	rc := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	routeSend, routeExpect := setupRoute(t, rc, opts)
	
	buf := routeExpect(connectRe)
	t.Log(string(buf))
	
	buf = routeExpect(infoRe)
	t.Log(string(buf))
	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if !info.AuthRequired {
		t.Fatal("Expected to see AuthRequired")
	}
	if info.Port != opts.RoutePort {
		t.Fatalf("Received wrong information for port, expected %d, got %d",
			info.Port, opts.RoutePort)
	}

	// Now send it back and make sure it is processed correctly inbound.
	routeSend(string(buf))
	routeSend("PING\r\n")
	routeExpect(pongRe)
}

func TestSendRouteSubAndUnsub(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	send, _ := setupConn(t, c)

	// We connect to the route.
	rc := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	buf := expectResult(t, c, infoRe)
	t.Log(string(buf))
	expectAuthRequired(t, rc)
	setupRoute(t, rc, opts)

	// Send SUB via client connection
	send("SUB foo 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf = expectResult(t, rc, subRe)
	matches := subRe.FindAllSubmatch(buf, -1)
	rsid := string(matches[0][5])
	if !strings.HasPrefix(rsid, "RSID:") {
		t.Fatalf("Got wrong RSID: %s\n", rsid)
	}

	// Send UNSUB via client connection
	send("UNSUB 22\r\n")

	// Make sure the SUB is broadcast via the route
	buf = expectResult(t, rc, unsubRe)
	matches = unsubRe.FindAllSubmatch(buf, -1)
	rsid2 := string(matches[0][1])

	if rsid2 != rsid {
		t.Fatalf("Expected rsid's to match. %q vs %q\n", rsid, rsid2)
	}
}

func TestRouteForwardsMsgFromClients(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	c, _, _ := zk.Connect(opts.ZkAddrs, time.Second)
	nodes, _, _ := c.Children(opts.ZkPath)
	tmps := strings.Split(nodes[0], "-")
	routeURL := "nats-route://" + tmps[0] + ":" + tmps[2]
	rUrl, _ := url.Parse(routeURL)
	
	route := acceptRouteConn(t, rUrl.Host, server.DEFAULT_ROUTE_CONNECT)
	defer route.Close()

	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Eat the CONNECT and INFO protos
	routeExpect(infoRe)

	// Send SUB via route connection
	routeSend("SUB foo RSID:2:22\r\n")
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "RSID:2:22", "", "2", "ok")
}

func TestRouteForwardsMsgToClients(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)
	expectMsgs := expectMsgsCommand(t, clientExpect)

	route := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	routeSend, routeExpect := setupRoute(t, route, opts)
	routeExpect(connectRe)
	expectAuthRequired(t, route)
	// Subscribe to foo
	clientSend("SUB foo 1\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Send MSG proto via route connection
	routeSend("MSG foo 1 2\r\nok\r\n")

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
}

func TestRouteOneHopSemantics(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	expectAuthRequired(t, route)
	routeSend, _ := setupRoute(t, route, opts)

	// Express interest on this route for foo.
	routeSend("SUB foo RSID:2:2\r\n")

	// Send MSG proto via route connection
	routeSend("MSG foo 1 2\r\nok\r\n")

	// Make sure it does not come back!
	expectNothing(t, route)
}

func TestRouteOnlySendOnce(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	defer client.Close()

	clientSend, clientExpect := setupConn(t, client)

	route := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo.
	routeSend("SUB foo RSID:2:1\r\n")
	routeSend("SUB foo RSID:2:2\r\n")
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "RSID:2:1", "", "2", "ok")
}

func TestRouteQueueSemantics(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	clientExpectMsgs := expectMsgsCommand(t, clientExpect)

	defer client.Close()

	route := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	expectAuthRequired(t, route)
	routeSend, routeExpect := setupRoute(t, route, opts)
	expectMsgs := expectMsgsCommand(t, routeExpect)

	// Express multiple interest on this route for foo, queue group bar.
	qrsid1 := "RSID:2:1"
	routeSend(fmt.Sprintf("SUB foo bar %s\r\n", qrsid1))
	qrsid2 := "RSID:2:2"
	routeSend(fmt.Sprintf("SUB foo bar %s\r\n", qrsid2))

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Only 1
	matches := expectMsgs(1)
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")

	// Add normal Interest as well to route interest.
	routeSend("SUB foo RSID:2:4\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// Should be 2 now, 1 for all normal, and one for specific queue subscriber.
	matches = expectMsgs(2)

	// Expect first to be the normal subscriber, next will be the queue one.
	checkMsg(t, matches[0], "foo", "RSID:2:4", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "", "", "2", "ok")

	// Check the rsid to verify it is one of the queue group subscribers.
	rsid := string(matches[1][SID_INDEX])
	if rsid != qrsid1 && rsid != qrsid2 {
		t.Fatalf("Expected a queue group rsid, got %s\n", rsid)
	}

	// Now create a queue subscription for the client as well as a normal one.
	clientSend("SUB foo 1\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)
	routeExpect(subRe)

	clientSend("SUB foo bar 2\r\n")
	// Use ping roundtrip to make sure its processed.
	clientSend("PING\r\n")
	clientExpect(pongRe)
	routeExpect(subRe)

	// Deliver a MSG from the route itself, make sure the client receives both.
	routeSend("MSG foo RSID:2:1 2\r\nok\r\n")
	// Queue group one.
	routeSend("MSG foo QRSID:2:2 2\r\nok\r\n")

	// Use ping roundtrip to make sure its processed.
	routeSend("PING\r\n")
	routeExpect(pongRe)

	// Should be 2 now, 1 for all normal, and one for specific queue subscriber.
	matches = clientExpectMsgs(2)
	// Expect first to be the normal subscriber, next will be the queue one.
	checkMsg(t, matches[0], "foo", "1", "", "2", "ok")
	checkMsg(t, matches[1], "foo", "2", "", "2", "ok")
}

func TestSolicitRouteReconnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	c, _, _ := zk.Connect(opts.ZkAddrs, time.Second)
	nodes, _, _ := c.Children(opts.ZkPath)
	tmps := strings.Split(nodes[0], "-")
	routeURL := "nats-route://" + tmps[0] + ":" + tmps[2]
	rUrl, _ := url.Parse(routeURL)

	route := acceptRouteConn(t, rUrl.Host, server.DEFAULT_ROUTE_CONNECT)

	// Go ahead and close the Route.
	route.Close()

	// We expect to get called back..
	route = acceptRouteConn(t, rUrl.Host, 2*server.DEFAULT_ROUTE_CONNECT)
}

func TestMultipleRoutesSameId(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	route1 := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	expectAuthRequired(t, route1)
	route1Send, _ := setupRouteEx(t, route1, opts, "ROUTE:2222")

	route2 := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	expectAuthRequired(t, route2)
	route2Send, _ := setupRouteEx(t, route2, opts, "ROUTE:2222")

	// Send SUB via route connections
	sub := "SUB foo RSID:2:22\r\n"
	route1Send(sub)
	route2Send(sub)

	// Make sure we do not get anything on a MSG send to a router.
	// Send MSG proto via route connection
	route1Send("MSG foo 1 2\r\nok\r\n")

	expectNothing(t, route1)
	expectNothing(t, route2)

	// Setup a client
	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)
	defer client.Close()

	// Send PUB via client connection
	clientSend("PUB foo 2\r\nok\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	// We should only receive on one route, not both.
	// Check both manually.
	route1.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf, _ := ioutil.ReadAll(route1)
	route1.SetReadDeadline(time.Time{})
	if len(buf) <= 0 {
		route2.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		buf, _ = ioutil.ReadAll(route2)
		route2.SetReadDeadline(time.Time{})
		if len(buf) <= 0 {
			t.Fatal("Expected to get one message on a route, received none.")
		}
	}

	matches := msgRe.FindAllSubmatch(buf, -1)
	if len(matches) != 1 {
		t.Fatalf("Expected 1 msg, got %d\n", len(matches))
	}
	checkMsg(t, matches[0], "foo", "", "", "2", "ok")
}

func TestRouteResendsLocalSubsOnReconnect(t *testing.T) {
	s, opts := runRouteServer(t)
	defer s.Shutdown()

	client := createClientConn(t, opts.Host, opts.Port)
	clientSend, clientExpect := setupConn(t, client)

	// Setup a local subscription
	clientSend("SUB foo 1\r\n")
	clientSend("PING\r\n")
	clientExpect(pongRe)

	route := createRouteConn(t, opts.RouteHost, opts.RoutePort)
	routeSend, routeExpect := setupRouteEx(t, route, opts, "ROUTE:4222")

	// Expect to see the local sub echoed through.
	buf := routeExpect(infoRe)
	
	// Generate our own so we can send one to trigger the local subs.
	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}
	info.ID = "ROUTE:4222"
	b, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("Could not marshal test route info: %v", err)
	}
	infoJson := fmt.Sprintf("INFO %s\r\n", b)

	routeSend(infoJson)
	routeExpect(subRe)

	// Close and re-open
	route.Close()

	route = createRouteConn(t, opts.RouteHost, opts.RoutePort)
	routeSend, routeExpect = setupRouteEx(t, route, opts, "ROUTE:4222")

	// Expect to see the local sub echoed through after info.
	routeExpect(infoRe)
	routeSend(infoJson)
	routeExpect(subRe)
}


// This will check that the matches include at least one of the sids. Useful for checking
// that we received messages on a certain queue group.
func checkForQueueSid(t tLogger, matches [][][]byte, sids []string) {
	seen := make(map[string]int, len(sids))
	for _, sid := range sids {
		seen[sid] = 0
	}
	for _, m := range matches {
		sid := string(m[SID_INDEX])
		if _, ok := seen[sid]; ok {
			seen[sid] += 1
		}
	}
	// Make sure we only see one and exactly one.
	total := 0
	for _, n := range seen {
		total += n
	}
	if total != 1 {
		stackFatalf(t, "Did not get a msg for queue sids group: expected 1 got %d\n", total)
	}
}

// This will check that the matches include all of the sids. Useful for checking
// that we received messages on all subscribers.
func checkForPubSids(t tLogger, matches [][][]byte, sids []string) {
	seen := make(map[string]int, len(sids))
	for _, sid := range sids {
		seen[sid] = 0
	}
	for _, m := range matches {
		sid := string(m[SID_INDEX])
		if _, ok := seen[sid]; ok {
			seen[sid] += 1
		}
	}
	// Make sure we only see one and exactly one for each sid.
	for sid, n := range seen {
		if n != 1 {
			stackFatalf(t, "Did not get a msg for sid[%s]: expected 1 got %d\n", sid, n)

		}
	}
}