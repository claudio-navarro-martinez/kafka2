package main

import (
        "fmt"
        "os"
        "os/signal"
        "github.com/Shopify/sarama"
)

func main() {
        brokerList := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
        topic := "oracle"

		config := sarama.NewConfig()
        config.Consumer.Offsets.Initial = sarama.OffsetNewest // al mas antiguo que no ja sido marcado como leido
        config.Consumer.Return.Errors = true

        consumer, err := sarama.NewConsumer(brokerList, config)

		client, err := sarama.NewClient(brokerList, config)
		defer client.Close()

		// pollon bendito, necesario para marcar offsets leidos 
		offsetManager, _ := sarama.NewOffsetManagerFromClient("oraclegroup", client)
        partitionOffsetManager, _ := offsetManager.ManagePartition(topic, 0)
        offset, _ := partitionOffsetManager.NextOffset()
        fmt.Println(offset)

		// client.getOffset devuelve el siguiente offset que se va a usar por un producer
        lastOffset, err := client.GetOffset(topic,0,sarama.OffsetNewest)
        fmt.Println(lastOffset)
        if err != nil {
                panic(err)
        }
		// aqui para sacar las partitions de un topic
        partitionList, _ := consumer.Partitions(topic)
        if err != nil {
                fmt.Println("Error retrieving partition List ", err)
        }
        fmt.Println(partitionList)
        // for this Oracle replication we must have only 1 partition to guarantee the order of the transactions
        pc, _ := consumer.ConsumePartition(topic, 0, offset+1)
        //fmt.Println(pc.HighWaterMarkOffset())
        defer pc.Close()
        defer consumer.Close()
        signals := make(chan os.Signal, 1)
        signal.Notify(signals, os.Interrupt)

        for {
                select {
                case msg, ok := <-pc.Messages():
                        if ok {
                                
                                if msg.Offset < 10 {
                                        partitionOffsetManager.MarkOffset(msg.Offset,"")
									}
									fmt.Println("Consumido:", string(msg.Topic), " ", msg.Partition, " ", msg.Offset)
							}
					case <-signals:
							return
					}
			}
	}
	
