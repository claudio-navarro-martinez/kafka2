package OracleKafkaConsumer

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	brokerList := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
	topic := "foo"

	// vamos a por los pipelines, input Kafka, output to stout
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // al mas antiguo que no ja sido marcado como leido
	config.Consumer.Return.Errors = true

	// topics := []string{"foo"}
	consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}

	partitionList, _ := consumer.Partitions(topic)
	if err != nil {
		fmt.Println("Error retrieving partition List ", err)
	}
	fmt.Println(partitionList)
	// for this Oracle replication we must have only 1 partition to guarantee the order of the transactions
	pc, _ := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)

	defer consumer.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg, ok := <-pc.Messages():
			if ok {
				// ahora hay que meter la transaccion en algun lugar :-) y marcarla como leida
				consumer.MarkOffset(msg, "")
				
				fmt.Println("Consumido:", string(msg.Topic), " ", msg.Partition, " ", msg.Offset)
			}
		case <-signals:
			return
		}
	}
}
