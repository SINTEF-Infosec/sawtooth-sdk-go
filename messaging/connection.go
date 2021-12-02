/**
 * Copyright 2017 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

// Package messaging handles lower-level communication between a transaction
// processor and validator.
package messaging

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/sawtooth-sdk-go/logging"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/validator_pb2"
	zmq "github.com/pebbe/zmq4"
	uuid "github.com/satori/go.uuid"
)

var logger = logging.Get()

// Generate a new UUID
func GenerateId() string {
	id, _ := uuid.NewV4()
	return id.String()
}

// DumpMsg serializes a validator message
func DumpMsg(t validator_pb2.Message_MessageType, c []byte, corrId string) ([]byte, error) {
	msg := &validator_pb2.Message{
		MessageType:   t,
		CorrelationId: corrId,
		Content:       c,
	}
	return proto.Marshal(msg)
}

// LoadMsg deserializes a validator message
func LoadMsg(data []byte) (msg *validator_pb2.Message, err error) {
	msg = &validator_pb2.Message{}
	err = proto.Unmarshal(data, msg)
	return
}

type Connection interface {
	SendData(id string, data []byte) error
	SendNewMsg(t validator_pb2.Message_MessageType, c []byte) (corrId string, err error)
	SendNewMsgTo(id string, t validator_pb2.Message_MessageType, c []byte) (corrId string, err error)
	SendMsg(t validator_pb2.Message_MessageType, c []byte, corrId string) error
	SendMsgTo(id string, t validator_pb2.Message_MessageType, c []byte, corrId string) error
	RecvData() (string, []byte, error)
	RecvMsg() (string, *validator_pb2.Message, error)
	RecvMsgWithId(corrId string) (string, *validator_pb2.Message, error)
	Close()
	Socket() *zmq.Socket
	Monitor(zmq.Event) (*zmq.Socket, error)
	Identity() string
}

// Connection wraps a ZMQ DEALER socket or ROUTER socket and provides some
// utility methods for sending and receiving messages.
type ZmqConnection struct {
	identity          string
	uri               string
	socket            *zmq.Socket
	context           *zmq.Context
	expectedMsg       chan *storedMsg
	unexpectedMsg     chan *storedMsg
	storedExpectedMsg map[string]*storedMsg
	registeredId      map[string]bool
}

type storedMsg struct {
	Id  string
	Msg *validator_pb2.Message
}

const (
	IncomingMsgBufferSize = 10_000
)

// NewConnection establishes a new connection using the given ZMQ context and
// socket type to the given URI.
func NewConnection(context *zmq.Context, t zmq.Type, uri string, bind bool) (*ZmqConnection, error) {
	socket, err := context.NewSocket(t)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZMQ socket: %v", err)
	}

	identity := GenerateId()
	if err := socket.SetIdentity(identity); err != nil {
		return nil, fmt.Errorf("failed to set socket identity: %v", err)
	}

	if bind {
		logger.Info("Binding to ", uri)
		err = socket.Bind(uri)
	} else {
		logger.Info("Connecting to ", uri)
		err = socket.Connect(uri)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to establish connection to %v: %v", uri, err)
	}

	conn := &ZmqConnection{
		identity:          identity,
		uri:               uri,
		socket:            socket,
		context:           context,
		storedExpectedMsg: make(map[string]*storedMsg),
		expectedMsg:       make(chan *storedMsg, IncomingMsgBufferSize),
		unexpectedMsg:     make(chan *storedMsg, IncomingMsgBufferSize),
		registeredId:      make(map[string]bool),
	}

	go conn.receiveData()

	return conn, nil
}

func (zc *ZmqConnection) receiveData() {
	for {
		// Receive a message from the socket
		id, bytes, err := zc.RecvData()
		if err != nil {
			logger.Errorf("could not received data: %v", err)
			continue
		}

		msg, err := LoadMsg(bytes)
		if err != nil {
			logger.Errorf("could not load msg: %v", err)
		}

		// check whether this corrId was expected or not and routes the msg to the right chan
		_, ok := zc.registeredId[msg.GetCorrelationId()]
		if ok {
			zc.expectedMsg <- &storedMsg{id, msg}
		} else {
			zc.unexpectedMsg <- &storedMsg{id, msg}
		}
	}
}

// SendData sends the byte array.
//
// If id is not "", the id is included as the first part of the message. This
// is useful for passing messages to a ROUTER socket so it can route them.
func (zc *ZmqConnection) SendData(id string, data []byte) error {
	if id != "" {
		_, err := zc.socket.SendMessage(id, [][]byte{data})
		if err != nil {
			return err
		}
	} else {
		_, err := zc.socket.SendMessage([][]byte{data})
		if err != nil {
			return err
		}
	}
	return nil
}

// SendNewMsg creates a new validator message, assigns a new correlation id,
// serializes it, and sends it. It returns the correlation id created.
func (zc *ZmqConnection) SendNewMsg(t validator_pb2.Message_MessageType, c []byte) (corrId string, err error) {
	return zc.SendNewMsgTo("", t, c)
}

// SendNewMsgTo sends a new message validator message with the given id sent as
// the first part of the message. This is required when sending to a ROUTER
// socket, so it knows where to route the message.
func (zc *ZmqConnection) SendNewMsgTo(id string, t validator_pb2.Message_MessageType, c []byte) (corrId string, err error) {
	corrId = GenerateId()
	zc.registeredId[corrId] = true
	return corrId, zc.SendMsgTo(id, t, c, corrId)
}

// Send a message with the given correlation id
func (zc *ZmqConnection) SendMsg(t validator_pb2.Message_MessageType, c []byte, corrId string) error {
	return zc.SendMsgTo("", t, c, corrId)
}

// Send a message with the given correlation id and the prepends the id like
// SendNewMsgTo()
func (zc *ZmqConnection) SendMsgTo(id string, t validator_pb2.Message_MessageType, c []byte, corrId string) error {
	data, err := DumpMsg(t, c, corrId)
	if err != nil {
		return err
	}

	return zc.SendData(id, data)
}

// RecvData receives a ZMQ message from the wrapped socket and returns the
// identity of the sender and the data sent. If ZmqConnection does not wrap a
// ROUTER socket, the identity returned will be "".
func (zc *ZmqConnection) RecvData() (string, []byte, error) {
	msg, err := zc.socket.RecvMessage(0)

	if err != nil {
		return "", nil, err
	}
	switch len(msg) {
	case 1:
		data := []byte(msg[0])
		return "", data, nil
	case 2:
		id := msg[0]
		data := []byte(msg[1])
		return id, data, nil
	default:
		return "", nil, fmt.Errorf(
			"Receive message with unexpected length: %v", len(msg),
		)
	}
}

// RecvMsg receives a new validator message and returns it deserialized. If
// ZmqConnection wraps a ROUTER socket, id will be the identity of the sender.
// Otherwise, id will be "".
func (zc *ZmqConnection) RecvMsg() (string, *validator_pb2.Message, error) {
	storedMsg := <- zc.unexpectedMsg
	return storedMsg.Id, storedMsg.Msg, nil
}

// RecvMsgWithId receives validator messages until a message with the given
// correlation id is found and returns this message. Any messages received that
// do not match the id are saved for subsequent receives.
func (zc *ZmqConnection) RecvMsgWithId(corrId string) (string, *validator_pb2.Message, error) {
	// If the message is already stored, just return it
	stored, exists := zc.storedExpectedMsg[corrId]
	if exists {
		delete(zc.storedExpectedMsg, corrId)
		return stored.Id, stored.Msg, nil
	}

	for {
		storedMsg := <- zc.expectedMsg

		if storedMsg.Msg != nil && storedMsg.Msg.GetCorrelationId() == corrId {
			return storedMsg.Id, storedMsg.Msg, nil
		} else if storedMsg.Msg != nil {
			zc.storedExpectedMsg[storedMsg.Msg.GetCorrelationId()] = storedMsg
		} else {
			logger.Errorf("received invalid message that is nil")
		}
	}
}

// Close closes the wrapped socket. This should be called with defer() after opening the socket.
func (zc *ZmqConnection) Close() {
	zc.socket.Close()
}

// Socket returns the wrapped socket.
func (zc *ZmqConnection) Socket() *zmq.Socket {
	return zc.socket
}

// Create a new monitor socket pair and return the socket for listening
func (zc *ZmqConnection) Monitor(events zmq.Event) (*zmq.Socket, error) {
	endpoint := fmt.Sprintf("inproc://monitor.%v", zc.identity)
	err := zc.socket.Monitor(endpoint, events)
	if err != nil {
		return nil, err
	}
	monitor, err := zc.context.NewSocket(zmq.PAIR)
	if err != nil {
		return nil, err
	}
	if err = monitor.Connect(endpoint); err != nil {
		return nil, err
	}
	return monitor, nil
}

// Identity returns the identity assigned to the wrapped socket.
func (zc *ZmqConnection) Identity() string {
	return zc.identity
}
