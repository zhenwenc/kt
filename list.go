package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
)

type Topic struct {
	Name       string
	Partitions []Partition
}

type Partition struct {
	Id       int32
	Leader   int32
	Replicas []int32
	Isrs     []int32
}

func printTopic(t *Topic) {
	fmt.Printf("%s\n", t.Name)
	for _, p := range t.Partitions {
		fmt.Printf("  partition: %d, leader: %d, replicas: %v\n", p.Id, p.Leader, p.Replicas)
	}
	return
}

func fetchTopic(topic string, client sarama.Client) (Topic, bool) {
	partitionIds, err := client.Partitions(topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read partitions, topic=%s err=%v\n", topic, err)
		return Topic{}, false
	}

	partitions := []Partition{}
	for _, partitionId := range partitionIds {
		partition, ok := fetchPartition(topic, partitionId, client)
		if !ok {
			fmt.Fprintf(os.Stderr, "Failed to read partition, topic=%s, partition=%d\n", topic, partitionId)
			return Topic{}, false
		}
		partitions = append(partitions, partition)
	}

	return Topic{Name: topic, Partitions: partitions}, true
}

func fetchPartition(topic string, partitionId int32, client sarama.Client) (Partition, bool) {
	leader, err := client.Leader(topic, partitionId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read leader, topic=%s, partition=%d, err=%v\n", topic, partitionId, err)
		return Partition{}, false
	}

	replicas, err := client.Replicas(topic, partitionId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read replica, topic=%s, partition=%d, err=%v\n", topic, partitionId, err)
		return Partition{}, false
	}

	return Partition{Id: partitionId, Leader: leader.ID(), Replicas: replicas}, true
}

func listCommand() command {
	list := flag.NewFlagSet("list", flag.ExitOnError)
	list.StringVar(&config.list.args.brokers, "brokers", "localhost:9092", "Comma separated list of brokers. Port defaults to 9092 when omitted.")
	list.BoolVar(&config.list.args.detail, "detail", false, "Show detail metadata for all topics. (default false).")

	list.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of list:")
		list.PrintDefaults()
		os.Exit(2)
	}

	return command{
		flags: list,
		parseArgs: func(args []string) {

			list.Parse(args)

			config.list.brokers = strings.Split(config.list.args.brokers, ",")
			for i, b := range config.list.brokers {
				if !strings.Contains(b, ":") {
					config.list.brokers[i] = b + ":9092"
				}
			}
		},

		run: func(closer chan struct{}) {

			client, err := sarama.NewClient(config.list.brokers, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to create client err=%v\n", err)
				os.Exit(1)
			}
			defer client.Close()

			topicNames, err := client.Topics()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read topics err=%v\n", err)
				os.Exit(1)
			}
			sort.Strings(topicNames)

			for _, topicName := range topicNames {
				if config.list.args.detail {
					topic, ok := fetchTopic(topicName, client)
					if ok {
						printTopic(&topic)
					}
				} else {
					fmt.Println(topicName)
				}
			}
		},
	}
}
