package raft

import "time"

// 用于抽象消息机制和过期机制的消息队列
type Message struct {
	MessageId   string
	MessageType string
	MessageBody interface{}
}
type Executor struct {
	Message     Message
	ExecuteTime time.Time
}
type MessageQuene struct {
	tick      time.Time
	Executors []*Executor
}

func (m *MessageQuene) Receive() {

}

func (m *MessageQuene) Append() {

}

func (m *MessageQuene) Start() {

}
