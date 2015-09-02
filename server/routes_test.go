package server

import (
	"reflect"
	"testing"
)

func TestRouteConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/cluster.conf")
	if err != nil {
		t.Fatalf("Received an error reading route config file: %v\n", err)
	}

	golden := &Options{
		Host:               "0.0.0.0",
		Port:               4222,
		Username:           "nats",
		Password:           "nats",
		ZkPath:				"/gnatsd",
		AuthTimeout:        1.0,
		LogFile:            "/export/home/jae/gnatsd.log",
		PidFile:            "/export/home/jae/gnatsd.pid",
		Trace:				true,
		Debug:				true,
		Logtime:			true,
		HTTPPort:			4224,
		RouteHost:			"0.0.0.0",
		RoutePort:			4223,
		ZkTimeout:			10,
		MaxProcs:			8,
	}

	golden.ZkAddrs = append(golden.ZkAddrs, "127.0.0.1:2181")
	golden.ZkAddrs = append(golden.ZkAddrs, "127.0.0.1:2182")
	golden.ZkAddrs = append(golden.ZkAddrs, "127.0.0.1:2183")
	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}
