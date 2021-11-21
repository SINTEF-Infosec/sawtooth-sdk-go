package consensus

import consensus_pb2 "github.com/hyperledger/sawtooth-sdk-go/protobuf/consensus_pb2"

type StartupState struct {
	ChainHead     *consensus_pb2.ConsensusBlock
	Peers         []*consensus_pb2.ConsensusPeerInfo
	LocalPeerInfo *consensus_pb2.ConsensusPeerInfo
}

type ProtocolInfo struct {
	Name    string
	Version string
}

type Engine interface {
	Start(updates chan Update, service Service, startUpState *StartupState)
	Stop()
	Name() string
	Version() string
	AdditionalProtocols() []ProtocolInfo
}
