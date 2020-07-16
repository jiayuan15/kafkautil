package producer

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/jiayuan15/kafkautil/logger"

	"github.com/Shopify/sarama"
	"gopkg.in/yaml.v2"
)

//MyProducer 定义生产者结构体
type MyProducer struct {
	Brokers          []string            `yaml:"brokers"`
	Sync             bool                `yaml:"sync"`
	WaitAck          sarama.RequiredAcks `yaml:"wait_ack"`
	Topic            string              `yaml:"topic"`
	LogOnlyToConsole bool                `yaml:"log_only_to_console"`
	SyncProducer     sarama.SyncProducer
	AsyncProducer    sarama.AsyncProducer
	log              *logger.Logger
}

//NewProducer 读配置文件初始化Producer
func NewProducer(path string) (myProducer *MyProducer, err error) {

	myProducer = new(MyProducer)

	pData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(pData, myProducer)
	if err != nil {
		return nil, err
	}

	myProducer.log = logger.NewProducerLogger("log", myProducer.LogOnlyToConsole)

	config := sarama.NewConfig()

	config.Producer.RequiredAcks = myProducer.WaitAck
	config.Producer.Partitioner = sarama.NewHashPartitioner
	//错误通道默认开启
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 5 * time.Second

	if myProducer.Sync {
		config.Producer.Return.Successes = true

		myProducer.SyncProducer, err = sarama.NewSyncProducer(myProducer.Brokers, config)
		if err != nil {
			return nil, err
		}
	} else {
		config.Producer.Return.Successes = true
		myProducer.AsyncProducer, err = sarama.NewAsyncProducer(myProducer.Brokers, config)
		if err != nil {
			fmt.Println(err.Error())
		}

		go func(myProducer *MyProducer) {
			for {
				select {
				case succMsg := <-myProducer.AsyncProducer.Successes():
					//fmt.Println(succMsg.Partition, succMsg.Offset, succMsg.Value)
					myProducer.log.Info("publish success at %d partition %d offset messsage %s", succMsg.Partition, succMsg.Offset, succMsg.Value)

				case errMsg := <-myProducer.AsyncProducer.Errors():
					//fmt.Println(errMsg.Msg.Partition, errMsg.Msg.Offset, errMsg.Msg.Value, errMsg.Err.Error())
					myProducer.log.Error("publish failed at %d partition %d offset text %s reason %s", errMsg.Msg.Partition, errMsg.Msg.Offset, errMsg.Msg.Value, errMsg.Err.Error())
				}
			}

		}(myProducer)

	}

	return myProducer, nil
}

//Publish 发送消息
func (myProducer *MyProducer) Publish(topic string, value string, key string) (partition int32, offset int64, err error) {

	if myProducer.Sync {
		partition, offset, err = myProducer.SyncPublish(topic, value, key)
	} else {
		myProducer.AsyncPublish(topic, value, key)
	}
	return

}

//SyncPublish 同步发送消息
func (myProducer *MyProducer) SyncPublish(topic string, value string, key string) (partition int32, offset int64, err error) {

	if len(key) > 0 {
		partition, offset, err = myProducer.SyncProducer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		})

	} else {
		partition, offset, err = myProducer.SyncProducer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		})

	}

	if err != nil {
		//fmt.Println(err.Error())
		myProducer.log.Error("publish failed at %d partition %d offset text %s reason %s", partition, offset, err.Error())
	}

	myProducer.log.Info("publish succsee at %d partition %d offset text %s", partition, offset, value)

	return

}

//AsyncPublish 异步发送消息
func (myProducer *MyProducer) AsyncPublish(topic string, value string, key string) {

	if len(key) > 0 {
		myProducer.AsyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}
	} else {
		myProducer.AsyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		}
	}

}

func (myProducer *MyProducer) close() {
	if myProducer.SyncProducer != nil {
		myProducer.SyncProducer.Close()
		myProducer.SyncProducer = nil
	}
	if myProducer.AsyncProducer != nil {
		myProducer.AsyncProducer.Close()
		myProducer.AsyncProducer = nil
	}

}
