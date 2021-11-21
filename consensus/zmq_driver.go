package consensus

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/sawtooth-sdk-go/messaging"
	consensus_pb2 "github.com/hyperledger/sawtooth-sdk-go/protobuf/consensus_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/validator_pb2"
	zmq "github.com/pebbe/zmq4"
	"log"
	"os"
	"time"
)

type Update struct {
	Type validator_pb2.Message_MessageType
	Data interface{}
}

const UpdateQueueSize = 100

type ZmQDriver struct {
	engine     Engine
	connection *messaging.ZmqConnection
	updates    chan Update
	exit       bool
}

func NewZmqDriver(engine Engine) *ZmQDriver {
	return &ZmQDriver{
		engine:  engine,
		updates: make(chan Update, UpdateQueueSize),
	}
}

func (z *ZmQDriver) Start(endpoint string) {
	zmqContext, err := zmq.NewContext()
	if err != nil {
		log.Printf("could not create ZMQ context: %v", err)
		os.Exit(-1)
	}
	conn, err := messaging.NewConnection(zmqContext, zmq.STREAM, endpoint, true)
	if err != nil {
		log.Printf("could not create new zmq connection: %v", err)
	}
	z.connection = conn

	startUpState, err := z.register()
	if err != nil {
		log.Printf("could not register: %v", err)
		os.Exit(-1)
	}

	if startUpState == nil {
		startUpState, err = z.waitUntilActive()
		if err != nil {
			log.Printf("failed to wait activation: %v", err)
			os.Exit(-1)
		}
	}

	// Start the main driver loop
	go z.work()

	// Start the underlying engine
	z.engine.Start(z.updates, NewZmqService(z.connection), startUpState)
}

func (z *ZmQDriver) Stop() {
	z.exit = true
	z.engine.Stop()
}

func (z *ZmQDriver) register() (*StartupState, error) {
	// ToDo: Wait for the connection to be ready

	request := &consensus_pb2.ConsensusRegisterRequest{
		Name:    z.engine.Name(),
		Version: z.engine.Version(),
	}

	data, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal: %v", err)
		return nil, err
	}

	for {
		time.Sleep(500 * time.Millisecond) // ToDo remove when timeout

		corId, err := z.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_REGISTER_REQUEST, data)
		if err != nil {
			log.Printf("could not send msg: %v", err)
			return nil, err
		}

		// Receives the corresponding response:
		// ToDo: add timeout
		_, msg, err := z.connection.RecvMsgWithId(corId)
		if err != nil {
			log.Printf("could not recv msg: %v", err)
			return nil, err
		}

		// Unmarshal in a ConsensusRegisterResponse
		var response consensus_pb2.ConsensusRegisterResponse
		err = proto.Unmarshal(msg.Content, &response)
		if err != nil {
			log.Printf("could not unmarshal consensus register response")
			return nil, err
		}

		if response.Status == consensus_pb2.ConsensusRegisterResponse_NOT_READY {
			continue
		}

		if response.Status == consensus_pb2.ConsensusRegisterResponse_OK {
			if response.ChainHead != nil && response.LocalPeerInfo != nil {
				return &StartupState{
					ChainHead:     response.ChainHead,
					Peers:         response.Peers,
					LocalPeerInfo: response.LocalPeerInfo,
				}, nil
			}

			return nil, nil
		}

		return nil, fmt.Errorf("registration failed with status %s", response.GetStatus())
	}
}

func (z *ZmQDriver) waitUntilActive() (*StartupState, error) {
	for {
		// Receives a message
		corId, msg, err := z.connection.RecvMsg()
		if err != nil {
			log.Printf("error while receiving message: %v", err)
			continue
		}

		// ToDO: handle consensus notify engine activated
		log.Printf("waiting for activation message, received message of type %v", msg.MessageType)

		ackData, err := proto.Marshal(&consensus_pb2.ConsensusNotifyAck{})
		if err != nil {
			log.Printf("could not prepare ack: %v", err)
			continue
		}
		msgData, err := proto.Marshal(&validator_pb2.Message{
			MessageType:   validator_pb2.Message_CONSENSUS_NOTIFY_ACK,
			Content:       ackData,
			CorrelationId: corId,
		})

		err = z.connection.SendMsg(validator_pb2.Message_CONSENSUS_NOTIFY_ACK, msgData, corId)
		if err != nil {
			log.Printf("could not send ack msg: %v", err)
			continue
		}
	}
}

func (z *ZmQDriver) work() {
	for {
		if z.exit {
			z.engine.Stop()
			break
		}

		// Receives a message
		corId, msg, err := z.connection.RecvMsg()
		if err != nil {
			log.Printf("error while receiving message: %v", err)
			continue
		}

		msgType, msgData, err := z.process(msg, corId)
		if err != nil {
			log.Printf("error while processing message: %v", err)
			continue
		}

		if msgType == validator_pb2.Message_PING_REQUEST {
			continue
		}

		// Putting the msgType and data to the queue
		z.updates <- Update{Type: msgType, Data: msgData}
	}
}

func (z *ZmQDriver) process(msg *validator_pb2.Message, corId string) (validator_pb2.Message_MessageType, interface{}, error) {
	tag := msg.MessageType
	var data interface{}

	if tag == validator_pb2.Message_CONSENSUS_NOTIFY_PEER_CONNECTED {
		var notification consensus_pb2.ConsensusNotifyPeerConnected
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.PeerInfo
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_PEER_DISCONNECTED {
		var notification consensus_pb2.ConsensusNotifyPeerDisconnected
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.PeerId
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_PEER_MESSAGE {
		var notification consensus_pb2.ConsensusNotifyPeerMessage
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.Message
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_BLOCK_NEW {
		var notification consensus_pb2.ConsensusNotifyBlockNew
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.Block
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_BLOCK_VALID {
		var notification consensus_pb2.ConsensusNotifyBlockValid
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.BlockId
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_BLOCK_INVALID {
		var notification consensus_pb2.ConsensusNotifyBlockInvalid
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.BlockId
	} else if tag == validator_pb2.Message_CONSENSUS_NOTIFY_BLOCK_COMMIT {
		var notification consensus_pb2.ConsensusNotifyBlockCommit
		err := proto.Unmarshal(msg.Content, &notification)
		if err != nil {
			return 0, nil, err
		}
		data = notification.BlockId
	} else if tag == validator_pb2.Message_PING_REQUEST {
		data = nil
	} else {
		return 0, nil, fmt.Errorf("unknown message type: %v", tag)
	}

	// Acking the msg
	ackData, err := proto.Marshal(&consensus_pb2.ConsensusNotifyAck{})
	if err != nil {
		return 0, nil, err
	}
	msgData, err := proto.Marshal(&validator_pb2.Message{
		MessageType:   validator_pb2.Message_CONSENSUS_NOTIFY_ACK,
		Content:       ackData,
		CorrelationId: corId,
	})

	err = z.connection.SendMsg(validator_pb2.Message_CONSENSUS_NOTIFY_ACK, msgData, corId)
	if err != nil {
		return 0, nil, err
	}

	return tag, data, nil
}
