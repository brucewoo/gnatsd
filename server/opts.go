// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/apcera/gnatsd/conf"
)

// Options block for gnatsd server.
type Options struct {
	Host               string        `json:"addr"`
	Port               int           `json:"port"`
	Trace              bool          `json:"-"`
	Debug              bool          `json:"-"`
	NoLog              bool          `json:"-"`
	NoSigs             bool          `json:"-"`
	Logtime            bool          `json:"-"`
	MaxConn            int           `json:"max_connections"`
	Username           string        `json:"user,omitempty"`
	Password           string        `json:"-"`
	Authorization      string        `json:"-"`
	PingInterval       time.Duration `json:"ping_interval"`
	MaxPingsOut        int           `json:"ping_max"`
	HTTPPort           int           `json:"http_port"`
	SslTimeout         float64       `json:"ssl_timeout"`
	AuthTimeout        float64       `json:"auth_timeout"`
	MaxControlLine     int           `json:"max_control_line"`
	MaxPayload         int           `json:"max_payload"`
	ZkAddrs        	   []string      `json:"addr"`
	ZkPath        	   string        `json:"path"`
	ZkTimeout 		   float64       `json:"zk_timeout"`
	RouteHost          string        `json:"route_addr"`
	RoutePort          int           `json:"route_port"`
	ProfPort           int           `json:"-"`
	PidFile            string        `json:"-"`
	LogFile            string        `json:"-"`
	MaxProcs		   int			 `json:"go_max_procs"`
}

type authorization struct {
	user    string
	pass    string
	timeout float64
}

type zkinfo struct {
	addr    string
	path    string
	timeout float64
}

// ProcessConfigFile processes a configuration file.
// FIXME(dlc): Hacky
func ProcessConfigFile(configFile string) (*Options, error) {
	opts := &Options{}

	if configFile == "" {
		return opts, nil
	}

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("error opening config file: %v", err)
	}

	m, err := conf.Parse(string(data))
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		switch strings.ToLower(k) {
		case "port":
			opts.Port = int(v.(int64))
		case "host", "net":
			opts.Host = v.(string)
		case "debug":
			opts.Debug = v.(bool)
		case "trace":
			opts.Trace = v.(bool)
		case "logtime":
			opts.Logtime = v.(bool)
		case "authorization":
			am := v.(map[string]interface{})
			auth := parseAuthorization(am)
			opts.Username = auth.user
			opts.Password = auth.pass
			opts.AuthTimeout = auth.timeout
		case "http_port", "monitor_port":
			opts.HTTPPort = int(v.(int64))
		case "cluster":
			cm := v.(map[string]interface{})
			if err := parseCluster(cm, opts); err != nil {
				return nil, err
			}
		case "logfile", "log_file":
			opts.LogFile = v.(string)
		case "pidfile", "pid_file":
			opts.PidFile = v.(string)
		case "go_max_procs":
			opts.MaxProcs = int(v.(int64))
		}
	}
	return opts, nil
}

// parseCluster will parse the cluster config.
func parseCluster(cm map[string]interface{}, opts *Options) error {
	for mk, mv := range cm {
		switch strings.ToLower(mk) {
		case "zkinfo":
			am := mv.(map[string]interface{})
			zkinfo := parseZkinfo(am)
			opts.ZkAddrs = strings.Split(zkinfo.addr, ",")
			opts.ZkPath = zkinfo.path
			opts.ZkTimeout = zkinfo.timeout
		case "route_port":
			opts.RoutePort = int(mv.(int64))
		case "route_host":
			opts.RouteHost = mv.(string)
		}
	}
	return nil
}

// Helper function to parse Authorization configs.
func parseAuthorization(am map[string]interface{}) authorization {
	auth := authorization{}
	for mk, mv := range am {
		switch strings.ToLower(mk) {
		case "user", "username":
			auth.user = mv.(string)
		case "pass", "password":
			auth.pass = mv.(string)
		case "timeout":
			at := float64(1)
			switch mv.(type) {
			case int64:
				at = float64(mv.(int64))
			case float64:
				at = mv.(float64)
			}
			auth.timeout = at
		}
	}
	return auth
}

// Helper function to parse zkinfo configs.
func parseZkinfo(am map[string]interface{}) zkinfo {
	zk := zkinfo{}
	for mk, mv := range am {
		switch strings.ToLower(mk) {
		case "addr":
			zk.addr = mv.(string)
		case "path":
			zk.path = mv.(string)
		case "timeout":
			at := float64(1)
			switch mv.(type) {
			case int64:
				at = float64(mv.(int64))
			case float64:
				at = mv.(float64)
			}
			zk.timeout = at
		}
	}
	return zk
}

// MergeOptions will merge two options giving preference to the flagOpts
// if the item is present.
func MergeOptions(fileOpts, flagOpts *Options) *Options {
	if fileOpts == nil {
		return flagOpts
	}
	if flagOpts == nil {
		return fileOpts
	}
	// Merge the two, flagOpts override
	opts := *fileOpts

	if flagOpts.Port != 0 {
		opts.Port = flagOpts.Port
	}
	if flagOpts.Host != "" {
		opts.Host = flagOpts.Host
	}
	if flagOpts.Username != "" {
		opts.Username = flagOpts.Username
	}
	if flagOpts.Password != "" {
		opts.Password = flagOpts.Password
	}
	if flagOpts.Authorization != "" {
		opts.Authorization = flagOpts.Authorization
	}
	if flagOpts.HTTPPort != 0 {
		opts.HTTPPort = flagOpts.HTTPPort
	}
	if flagOpts.Debug {
		opts.Debug = true
	}
	if flagOpts.Trace {
		opts.Trace = true
	}
	if flagOpts.LogFile != "" {
		opts.LogFile = flagOpts.LogFile
	}
	if flagOpts.PidFile != "" {
		opts.PidFile = flagOpts.PidFile
	}
	return &opts
}

func processOptions(opts *Options) {
	// Setup non-standard Go defaults
	if opts.Host == "" {
		opts.Host = DEFAULT_HOST
	}
	if opts.Port == 0 {
		opts.Port = DEFAULT_PORT
	}
	if opts.MaxConn == 0 {
		opts.MaxConn = DEFAULT_MAX_CONNECTIONS
	}
	if opts.PingInterval == 0 {
		opts.PingInterval = DEFAULT_PING_INTERVAL
	}
	if opts.MaxPingsOut == 0 {
		opts.MaxPingsOut = DEFAULT_PING_MAX_OUT
	}
	if opts.SslTimeout == 0 {
		opts.SslTimeout = float64(SSL_TIMEOUT) / float64(time.Second)
	}
	if opts.AuthTimeout == 0 {
		opts.AuthTimeout = float64(AUTH_TIMEOUT) / float64(time.Second)
	}
	if opts.MaxControlLine == 0 {
		opts.MaxControlLine = MAX_CONTROL_LINE_SIZE
	}
	if opts.MaxPayload == 0 {
		opts.MaxPayload = MAX_PAYLOAD_SIZE
	}
	if opts.RouteHost == "" {
		opts.RouteHost = DEFAULT_HOST
	}
	if opts.MaxProcs == 0 {
		opts.MaxProcs = DEFAULT_MAX_PROCS
	}
}
