package consensus

import (
	consensus_pb2 "github.com/hyperledger/sawtooth-sdk-go/protobuf/consensus_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/validator_pb2"
)

type Block struct {
	BlockId    []byte
	PreviousId []byte
	SignerId   []byte
	BlockNum   []byte
	Payload    []byte
	Summary    []byte
}

type Service interface {
	SendTo(receiverId []byte, messageType validator_pb2.Message_MessageType, payload []byte)
	Broadcast(messageType validator_pb2.Message_MessageType, payload []byte)
	InitializeBlock(previousId []byte)
	SummarizeBlock() []byte
	FinalizeBlock(data []byte) []byte
	CancelBlock()
	CheckBlocks(priority [][]byte)
	CommitBlocks(blockId []byte)
	IgnoreBlock(blockId []byte)
	FailBlock(blockId []byte)
	GetBlocks(blockIds [][]byte) []*consensus_pb2.ConsensusBlock
	GetChainHead() *consensus_pb2.ConsensusBlock
	GetSettings(blockId []byte, settings []string) []*consensus_pb2.ConsensusSettingsEntry
	GetState(blockId []byte, addresses []string) []*consensus_pb2.ConsensusStateEntry
}
