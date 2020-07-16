package consumer

import (
	"io/ioutil"
	"sync"
	"time"

	"github.com/jiayuan15/kafkautil/logger"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"gopkg.in/yaml.v2"
)

//MyConsumer 消费者结构体
type MyConsumer struct {
	Brokers          []string `yaml:"brokers"`
	Topics           []string `yaml:"topics"`
	Count            int      `yaml:"count"`
	Group            string   `yaml:"group"`
	LogOnlyToConsole bool     `yaml:"log_only_to_console"`
	log              *logger.Logger
}

//NewConsumer 初始化消费者
func NewConsumer(path string) (myConsumer *MyConsumer, err error) {

	myConsumer = new(MyConsumer)

	cData, err := ioutil.ReadFile(path)

	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(cData, myConsumer)
	if err != nil {
		return nil, err
	}
	myConsumer.log = logger.NewConsumerLogger("log", myConsumer.LogOnlyToConsole)

	return myConsumer, nil
}

//Consume 消费者方法
func (myConsumer *MyConsumer) Consume(fn func(msg *sarama.ConsumerMessage) error) {

	wg := sync.WaitGroup{}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true                                    //返回错误信息
	config.Group.Return.Notifications = true                                //返回重新平衡通知信息
	config.Consumer.Offsets.Initial = sarama.OffsetNewest                   //第一次从较新的offset开始
	config.Consumer.Offsets.CommitInterval = 1 * time.Second                //提交位移时间间隔
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky //分区分配策略

	for i := 1; i <= myConsumer.Count; i++ {
		wg.Add(1)
		go myConsumer.getMessage(config, i, fn)

	}
	wg.Wait()

}

//获取消息
func (myConsumer *MyConsumer) getMessage(config *cluster.Config, index int, fn func(msg *sarama.ConsumerMessage) error) {

	consumer, err := cluster.NewConsumer(myConsumer.Brokers, myConsumer.Group, myConsumer.Topics, config)

	if err != nil {
		myConsumer.log.Error("consumer group %s id %d err %s", myConsumer.Group, index, err.Error())
	}
	defer consumer.Close()

	//错误通道
	go func() {
		for err = range consumer.Errors() {
			//fmt.Println("consumer get meaasge err", myConsumer.Group, index, err.Error())
			myConsumer.log.Error("consumer group %s id %d err %s", myConsumer.Group, index, err.Error())
		}
	}()

	//读nft数据
	go func() {
		for nft := range consumer.Notifications() {
			//fmt.Println("consumer nft", myConsumer.Group, index, nft.Type.String())
			myConsumer.log.Info("consumer group %s id %d nft %s", myConsumer.Group, index, nft.Type.String())
		}
	}()

	for msg := range consumer.Messages() {
		consumer.MarkOffset(msg, "")

		if err = fn(msg); err != nil {
			myConsumer.log.Error("consumer group %s id %d handle topic=%s partition=%d offset=%d value=%s has error %s", myConsumer.Group, index, msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err.Error())
		}

		myConsumer.log.Info("consumer group %s id %d msg topic=%s partition=%d offset=%d value=%s", myConsumer.Group, index, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	}

}
