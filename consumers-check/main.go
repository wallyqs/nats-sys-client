package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	nsys "github.com/piotrpio/nats-sys-client/pkg/sys"
)

type ConsumerDetail struct {
	StreamName           string
	ConsumerName         string
	Account              string
	RaftGroup            string
	State                nats.StreamState
	Cluster              *nats.ClusterInfo
	StreamCluster        *nats.ClusterInfo
	DeliveredStreamSeq   uint64
	DeliveredConsumerSeq uint64
	AckFloorStreamSeq    uint64
	AckFloorConsumerSeq  uint64
	NumAckPending        int
	NumRedelivered       int
	NumWaiting           int
	NumPending           uint64
}

func main() {
	log.SetFlags(0)
	var urls, sname, cname string
	var creds string
	var timeout int
	var unsyncedFilter bool
	flag.StringVar(&urls, "s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	flag.StringVar(&creds, "creds", "", "The NATS credentials")
	flag.StringVar(&sname, "stream", "", "Select a single stream")
	flag.StringVar(&cname, "consumer", "", "Select a single consumer")
	flag.IntVar(&timeout, "timeout", 30, "Connect timeout")
	flag.BoolVar(&unsyncedFilter, "unsynced", false, "Filter by streams that are out of sync")
	flag.Parse()

	start := time.Now()

	opts := []nats.Option{
		nats.Timeout(time.Duration(timeout) * time.Second),
	}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}

	nc, err := nats.Connect(urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected in %.3fs", time.Since(start).Seconds())
	sys := nsys.NewSysClient(nc)

	start = time.Now()
	servers, err := sys.JszPing(nsys.JszEventOptions{
		JszOptions: nsys.JszOptions{
			Streams:    true,
			Consumer:   true,
			RaftGroups: true,
		},
	})
	log.Printf("Response took %.3fs", time.Since(start).Seconds())
	if err != nil {
		log.Fatal(err)
	}
	header := fmt.Sprintf("Servers: %d", len(servers))
	fmt.Println(header)

	consumers := make(map[string]map[string]*ConsumerDetail)
	// Collect all info from servers.
	for _, resp := range servers {
		server := resp.Server
		jsz := resp.JSInfo
		for _, acc := range jsz.AccountDetails {
			for _, stream := range acc.Streams {
				for _, consumer := range stream.Consumer {
					var ok bool
					var m map[string]*ConsumerDetail
					if m, ok = consumers[consumer.Name]; !ok {
						m = make(map[string]*ConsumerDetail)
						consumers[consumer.Name] = m
					}
					var raftGroup string
					for _, cr := range stream.ConsumerRaftGroups {
						if cr.Name == consumer.Name {
							raftGroup = cr.RaftGroup
							break
						}
					}

					m[server.Name] = &ConsumerDetail{
						StreamName:           consumer.Stream,
						ConsumerName:         consumer.Name,
						Account:              acc.Name,
						RaftGroup:            raftGroup,
						State:                stream.State,
						DeliveredStreamSeq:   consumer.Delivered.Consumer,
						DeliveredConsumerSeq: consumer.Delivered.Consumer,
						AckFloorStreamSeq:    consumer.AckFloor.Stream,
						AckFloorConsumerSeq:  consumer.AckFloor.Consumer,
						Cluster:              consumer.Cluster,
						StreamCluster:        stream.Cluster,
						NumAckPending:        consumer.NumAckPending,
						NumRedelivered:       consumer.NumRedelivered,
						NumWaiting:           consumer.NumWaiting,
						NumPending:           consumer.NumPending,
					}
				}
			}
		}
	}
	keys := make([]string, 0)
	for k := range consumers {
		for kk := range consumers[k] {
			key := fmt.Sprintf("%s/%s", k, kk)
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	line := strings.Repeat("-", 170)
	fmt.Printf("Consumers: %d\n", len(keys))
	fmt.Println()

	fields := []any{"CONSUMER", "STREAM", "RAFT", "ACCOUNT", "NODE", "DELIVERED", "ACK_FLOOR", "COUNTERS", "STATUS"}
	fmt.Printf("%-10s %-15s %-15s %-10s %-15s | %-20s | %-20s | %-20s | %-30s\n", fields...)

	var prev string
	for i, k := range keys {
		var unsynced bool
		v := strings.Split(k, "/")
		consumerName, serverName := v[0], v[1]
		if cname != "" && consumerName != cname {
			continue
		}
		consumer := consumers[consumerName]
		replica := consumer[serverName]
		var status string
		statuses := make(map[string]bool)

		// Make comparisons against other peers.
		for _, peer := range consumer {
			if peer.DeliveredStreamSeq != replica.DeliveredStreamSeq &&
				peer.DeliveredConsumerSeq != replica.DeliveredConsumerSeq {
				statuses["UNSYNCED:DELIVERED"] = true
				unsynced = true
			}
			if peer.AckFloorStreamSeq != replica.AckFloorStreamSeq &&
				peer.AckFloorConsumerSeq != replica.AckFloorConsumerSeq {
				statuses["UNSYNCED:ACK_FLOOR"] = true
				unsynced = true
			}
			if peer.Cluster.Leader != replica.Cluster.Leader {
				statuses["MULTILEADER"] = true
				unsynced = true
			}
		}
		if replica.AckFloorStreamSeq == 0 || replica.AckFloorConsumerSeq == 0 ||
			replica.DeliveredConsumerSeq == 0 || replica.DeliveredStreamSeq == 0 {
			statuses["LOST STATE"] = true
			unsynced = true
		}
		if len(statuses) > 0 {
			for k, _ := range statuses {
				status = fmt.Sprintf("%s%s,", status, k)
			}
		} else {
			status = "IN SYNC"
		}

		if unsyncedFilter && !unsynced {
			continue
		}
		if i > 0 && prev != consumerName {
			fmt.Println(line)
		}

		sf := make([]any, 0)
		sf = append(sf, replica.ConsumerName)
		sf = append(sf, replica.StreamName)
		sf = append(sf, replica.RaftGroup)
		sf = append(sf, replica.Account)

		// Mark it in case it is a leader.
		var suffix string
		if serverName == replica.Cluster.Leader {
			suffix = "*"
		} else if replica.Cluster.Leader == "" {
			status = "LEADERLESS"
			unsynced = true
		}
		s := fmt.Sprintf("%s%s", serverName, suffix)
		sf = append(sf, s)
		sf = append(sf, fmt.Sprintf("%d | %d", replica.DeliveredStreamSeq, replica.DeliveredConsumerSeq))
		sf = append(sf, fmt.Sprintf("%d | %d", replica.AckFloorStreamSeq, replica.AckFloorConsumerSeq))
		sf = append(sf, fmt.Sprintf("(ap:%d, nr:%d, nw:%d, np:%d)",
			replica.NumAckPending,
			replica.NumRedelivered,
			replica.NumWaiting,
			replica.NumPending,
		))
		sf = append(sf, fmt.Sprintf("leader: %s", replica.Cluster.Leader))
		sf = append(sf, status)
		fmt.Printf("%-10s %-15s %-15s %-10s %-15s | %-20s | %-20s | %-20s | %-12s | %s \n", sf...)

		prev = consumerName
	}
}
