package main

import (
   "fmt"
   "github.com/Shopify/sarama"
   "log"
   "time"
)

func main() {
    config := sarama.NewConfig()
    config.Version = sarama.V2_4_0_0
    config.Producer.Return.Successes = true
    config.Producer.RequiredAcks = sarama.WaitForAll
    // config.Producer.Flush.Frequency = 10 * time.Second
    // config.Producer.Flush.Bytes = 1024 * 1024
    // config.Producer.Flush.MaxMessages = 1024
    brokerList := []string{"localhost:19092","localhost:29092","localhost:39092"}
    topic := "foo" 
    producer, err := sarama.NewAsyncProducer(strings.Split(*brokers, ","), config)
    if err != nil {
        panic(err)
    }

    // Trap SIGINT to trigger a graceful shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    var (
        wg  sync.WaitGroup
        enqueued, successes, errors int
    )

    wg.Add(1)
    go func() {
        defer wg.Done()
        for range producer.Successes() {
            successes++
        }
    }()

    wg.Add(1)
    go func() {
        defer wg.Done()
        for err := range producer.Errors() {
            log.Println(err)
            errors++
        }
    }()
    counter := 0

ProducerLoop:
    for {
        if counter >= 65536 {
            producer.AsyncClose() // Trigger a shutdown of the producer.
            break ProducerLoop
        }
        message := &sarama.ProducerMessage{
            Topic: topic,
            // Key:       sarama.StringEncoder(fmt.Sprintf("%d", counter)),
            // Partition: int32(counter),
            Value: sarama.StringEncoder(fmt.Sprintf("%d,%d", counter, time.Now().UnixNano())),
            // Timestamp: time.Now(),
        }
        select {
        	case producer.Input() <- message:
            		enqueued++

        	case <-signals:
            		producer.AsyncClose() // Trigger a shutdown of the producer.
            		break ProducerLoop
        }
        if *sleep {
            // fmt.Println(100 * time.Millisecond)
            time.Sleep(1 * time.Millisecond)
        }
        counter++
    }

    wg.Wait()

    log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}

