package logger

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

//Logger 日志采集器
type Logger struct {
	std              *log.Logger
	info             *log.Logger
	err              *log.Logger
	Logsuccess       bool
	LogOnlyToConsole bool
}

//NewProducerLogger 初始化生产者日志采集器
func NewProducerLogger(path string, logOnlyToConsole bool) *Logger {
	flag := log.Ldate | log.Lmicroseconds | log.Lshortfile

	l := new(Logger)

	l.LogOnlyToConsole = logOnlyToConsole

	//控制台输出
	l.std = log.New(os.Stdout, "", flag)

	//生产者info日志
	l.info = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/producer.info.log", path),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I]", flag)

	//生产者error日志
	l.err = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/producer.err.log", path),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E]", flag)

	return l
}

//NewConsumerLogger 初始化消费者日志采集器
func NewConsumerLogger(path string, logOnlyToConsole bool) *Logger {
	flag := log.LstdFlags | log.Lshortfile

	l := new(Logger)

	l.LogOnlyToConsole = logOnlyToConsole

	//控制台输出
	l.std = log.New(os.Stdout, "", flag)

	//生产者info日志
	l.info = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/consumer.info.log", path),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[I]", flag)

	//生产者error日志
	l.err = log.New(&lumberjack.Logger{
		Filename:  fmt.Sprintf("%s/consumer.err.log", path),
		MaxSize:   50,
		MaxAge:    3,
		LocalTime: true,
		Compress:  false,
	}, "[E]", flag)

	return l
}

//Info 打印info日志
func (l *Logger) Info(format string, v ...interface{}) {

	l.std.Output(3, fmt.Sprintf("[I]"+format, v...))

	if l.LogOnlyToConsole {
		return
	}

	l.info.Output(3, fmt.Sprintf(format, v...))

}

//Error 打印 error 日志
func (l *Logger) Error(format string, v ...interface{}) {

	l.std.Output(3, fmt.Sprintf("[E]"+format, v...))

	if l.LogOnlyToConsole {
		return
	}

	l.info.Output(3, fmt.Sprintf(format, v...))

}
