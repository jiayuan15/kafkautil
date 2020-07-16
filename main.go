package main

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/jiayuan15/kafkautil/consumer"
)

func main() {

	// myProducer, err := producer.NewProducer("conf/producer.yaml")
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }

	// // var partition int32
	// // var offset int64
	// for i := 0; i < 100; i++ {
	// 	value := fmt.Sprintf("jia%d", i)
	// 	myProducer.Publish(myProducer.Topic, value, "")
	// 	//fmt.Println(partition, offset, value)
	// }

	// if err != nil {
	// 	fmt.Println(err.Error())
	// }

	// select {}

	myConsumer, err := consumer.NewConsumer("conf/consumer.yaml")
	if err != nil {
		fmt.Println(err.Error())
	},

	for {
		myConsumer.Consume(magMethod)
		// time.Sleep(2 * time.Second)
	}

}

func magMethod(msg *sarama.ConsumerMessage) error {
	//fmt.Println(msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	return nil
}
