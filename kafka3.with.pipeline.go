// module github.com/claudio-navarro-martinez/pipeline-go
//
//go 1.12
//
//require (
//        github.com/Shopify/sarama v1.24.1
//        github.com/bsm/sarama-cluster v2.1.15+incompatible
//)
//
package main

import (
        "log"
        "fmt"
        "time"
        "os"
        "os/signal"
        "github.com/Shopify/sarama"
        "github.com/bsm/sarama-cluster"
)

func main() {
        brokerList := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
        // TxChan := make(chan *sarama.ConsumerMessage)
        TxChan := make(chan []byte)
        topic := "foo"

                // produciendo mensagges a saco hacia Kafka, uno cada 3 segundos
        go ProduceMessageAsync(brokerList, topic)

        // vamos a por los pipelines, input Kafka, output to stout
        config := cluster.NewConfig()
        config.Consumer.Offsets.Initial = sarama.OffsetOldest
        config.Consumer.Return.Errors = true
        config.Group.Return.Notifications = true

        topics := []string{"foo"}
        consumer, err := cluster.NewConsumer(brokerList,"eMumba",topics, config)
        if err != nil {
                panic(err)
        }
        defer consumer.Close()
        signals := make(chan os.Signal,1)
        signal.Notify(signals, os.Interrupt)

        // gorutine para leer del pipeline, sale del bucle si le mandamos una signal
        go func() {
                for {
                        select {
                                case output := <- TxChan:
                                        fmt.Println("Recibido:", string(output))
                                case <- signals:
                                        return
                        }

                }
        }()

        go func() {
                for err := range consumer.Errors() {
                        fmt.Println("Error: ", err)
                }
        }()

        go func() {
                for ntf := range consumer.Notifications() {
                        fmt.Println("Rebalanced: ", ntf)
                }
        }()

        for {
                select {
                        case msg, ok := <-consumer.Messages():
                          if ok {
                                        // aqui empieza lo bueno
                                        // para cada msg consumido desde Kafka, lo meto en el pipeline
                                        consumer.MarkOffset(msg,"")

                                        TxChan <- msg.Value
                          }
                        case <- signals:
                          return
                }
        }
}

func ProduceMessageAsync(brokerlist []string, topic string) {
        config := sarama.NewConfig()
        config.Producer.RequiredAcks = sarama.WaitForLocal
        config.Producer.Compression = sarama.CompressionSnappy
        config.Producer.Flush.Frequency = 500 * time.Millisecond

        p, err := sarama.NewAsyncProducer(brokerlist, config)
        if err != nil {
                        log.Fatalln("failed to start Sarama Producer:", err)
        }
        go func() {
                        for err := range p.Errors() {
                                        log.Println("failed to write access log entry:", err)
                        }
        }()

        for now := range time.Tick(time.Millisecond * 3000) {
                        myvalue := now.String()
                        message := &sarama.ProducerMessage{
                                Topic: topic,
                                Value: sarama.StringEncoder("Topic" + topic + " now: "+myvalue),
                        }
                        fmt.Println(message.Value)
                        p.Input() <- message
        }
}

