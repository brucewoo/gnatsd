// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Connz represents detail information on current connections.
type Connz struct {
	NumConns int         `json:"num_connections"`
	Conns    []*ConnInfo `json:"connections"`
}

// ConnInfo has detailed information on a per connection basis.
type ConnInfo struct {
	Cid      uint64 `json:"cid"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Subs     uint32 `json:"subscriptions"`
	Pending  int    `json:"pending_size"`
	InMsgs   int64  `json:"in_msgs"`
	OutMsgs  int64  `json:"out_msgs"`
	InBytes  int64  `json:"in_bytes"`
	OutBytes int64  `json:"out_bytes"`
	Subjects string `json:"subject"`
	Type     string `json:"type"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleConnz(w http.ResponseWriter, r *http.Request) {
	c := Connz{Conns: []*ConnInfo{}}

	// Walk the list
	s.mu.Lock()
	for _, client := range s.clients {
		ci := &ConnInfo{
			Cid:      client.cid,
			Subs:     client.subs.Count(),
			InMsgs:   client.inMsgs,
			OutMsgs:  client.outMsgs,
			InBytes:  client.inBytes,
			OutBytes: client.outBytes,
		}
		
		client.mu.Lock()
		subs := client.subs.All()
		client.mu.Unlock()
		for _, s := range subs {
			if sub, ok := s.(*subscription); ok {
				subject := string(sub.subject)
				if !strings.Contains(subject, "_INBOX") {
					ci.Subjects += "[" + subject + "]"
				}
			}
		}
		
		port := ""
		ci.IP, port = client.ConnStr()
		ci.Port, _ = strconv.Atoi(port)
		ci.Type = "Client"

		c.Conns = append(c.Conns, ci)
	}
	
	for _, route := range s.routes {
		ci := &ConnInfo {
			Cid:      route.cid,
			Subs:     route.subs.Count(),
			InMsgs:   route.inMsgs,
			OutMsgs:  route.outMsgs,
			InBytes:  route.inBytes,
			OutBytes: route.outBytes,
		}
		
		route.mu.Lock()
		routeSubs := route.subs.All()
		route.mu.Unlock()
		for _, s := range routeSubs {
			if sub, ok := s.(*subscription); ok {
				subject := string(sub.subject)
				if !strings.Contains(subject, "_INBOX") {
					ci.Subjects += "[" + subject + "]"
				}
			}
		}
		
		port := ""
		ci.IP, port = route.ConnStr()
		ci.Port, _ = strconv.Atoi(port)
		ci.Type = "Router"

		c.Conns = append(c.Conns, ci)
	}
	s.mu.Unlock()

	c.NumConns = len(c.Conns)
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		Log("Error marshalling response go /connzz request: %v", err)
	}
	w.Write(b)
}

// Connz represents detail information on current connections.
type Subz struct {
	NumClients int       `json:"num_clients"`
	Subs    []*SubInfo 	 `json:"clients"`
}

// ConnInfo has detailed information on a per connection basis.
type SubInfo struct {
	Cid      uint64 `json:"cid"`
	IP       string `json:"ip"`
	Port     int    `json:"port"`
	Type     string `json:"type"`
}

// HandleConnz process HTTP requests for connection information.
func (s *Server) HandleSubz(w http.ResponseWriter, r *http.Request) {
	subject := r.FormValue("subject")
	
	subs := Subz{Subs: []*SubInfo{}}
	matchSubs := s.sl.Match([]byte(subject))
	
	for _, v := range matchSubs {
		sub := v.(*subscription)
		si := &SubInfo{
			Cid:      sub.client.cid,
		}
		port := ""
		si.IP, port = sub.client.ConnStr()
		si.Port, _ = strconv.Atoi(port)
		si.Type = sub.client.typeString()
		
		subs.Subs = append(subs.Subs, si)
	}
	subs.NumClients = len(subs.Subs)
	b, err := json.MarshalIndent(subs, "", "  ")
	if err != nil {
		Log("Error marshalling response go /connzz request: %v", err)
	}
	w.Write(b)
}