package consensus

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/sawtooth-sdk-go/messaging"
	consensus_pb2 "github.com/hyperledger/sawtooth-sdk-go/protobuf/consensus_pb2"
	"github.com/hyperledger/sawtooth-sdk-go/protobuf/validator_pb2"
	"log"
)

type ZmqService struct {
	connection messaging.Connection
}

func NewZmqService(connection messaging.Connection) *ZmqService {
	return &ZmqService{
		connection: connection,
	}
}

// ---------------- P2P ------------------------

func (zs *ZmqService) SendTo(receiverId []byte, messageType validator_pb2.Message_MessageType, payload []byte) {
	request := &consensus_pb2.ConsensusSendToRequest{
		PeerId: receiverId,
		Message: &consensus_pb2.ConsensusPeerMessage{
			MessageType: messageType.String(),
			Content:     payload,
		},
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsgTo(string(receiverId), messageType, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusSendToResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status != consensus_pb2.ConsensusSendToResponse_OK {
		log.Printf("could not send message: %v", msg.MessageType)
	}
}

func (zs *ZmqService) Broadcast(messageType validator_pb2.Message_MessageType, payload []byte) {
	request := &consensus_pb2.ConsensusBroadcastRequest{
		Message: &consensus_pb2.ConsensusPeerMessage{
			MessageType: messageType.String(),
			Content:     payload,
		},
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(messageType, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusBroadcastResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status != consensus_pb2.ConsensusBroadcastResponse_OK {
		log.Printf("could not send message: %v", msg.MessageType)
	}
}

// ---------------- Block creation ------------------------

func (zs *ZmqService) InitializeBlock(previousId []byte) {
	request := &consensus_pb2.ConsensusInitializeBlockRequest{}

	if len(previousId) > 0 {
		request.PreviousId = previousId
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_INITIALIZE_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusInitializeBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusInitializeBlockResponse_INVALID_STATE {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return
	}

	if response.Status == consensus_pb2.ConsensusInitializeBlockResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown bloxk: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusInitializeBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

func (zs *ZmqService) SummarizeBlock() []byte {
	request := &consensus_pb2.ConsensusSummarizeBlockRequest{}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_SUMMARIZE_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusSummarizeBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusSummarizeBlockResponse_INVALID_STATE {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusSummarizeBlockResponse_BLOCK_NOT_READY {
		log.Printf("could not initialize block, block not ready: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusSummarizeBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.Summary
}

func (zs *ZmqService) FinalizeBlock(data []byte) []byte {
	request := &consensus_pb2.ConsensusFinalizeBlockRequest{
		Data: data,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_FINALIZE_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusFinalizeBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusFinalizeBlockResponse_INVALID_STATE {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusFinalizeBlockResponse_BLOCK_NOT_READY {
		log.Printf("could not initialize block, block not ready: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusFinalizeBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.BlockId
}

func (zs *ZmqService) CancelBlock() {
	request := &consensus_pb2.ConsensusCancelBlockRequest{}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_CANCEL_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusCancelBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusCancelBlockResponse_INVALID_STATE {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusCancelBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

func (zs *ZmqService) CheckBlocks(priority [][]byte) {
	request := &consensus_pb2.ConsensusCheckBlocksRequest{
		BlockIds: priority,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_CHECK_BLOCKS_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusCheckBlocksResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusCheckBlocksResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusCheckBlocksResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

func (zs *ZmqService) CommitBlocks(blockId []byte) {
	request := &consensus_pb2.ConsensusCommitBlockRequest{
		BlockId: blockId,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_COMMIT_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusCommitBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusCommitBlockResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, invalid state: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusCommitBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

func (zs *ZmqService) IgnoreBlock(blockId []byte) {
	request := &consensus_pb2.ConsensusIgnoreBlockRequest{
		BlockId: blockId,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_IGNORE_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusIgnoreBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusIgnoreBlockResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown block: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusIgnoreBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

func (zs *ZmqService) FailBlock(blockId []byte) {
	request := &consensus_pb2.ConsensusFailBlockRequest{
		BlockId: blockId,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_FAIL_BLOCK_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return
	}

	var response consensus_pb2.ConsensusFailBlockResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return
	}

	if response.Status == consensus_pb2.ConsensusFailBlockResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown block: %v", msg.MessageType)
		return
	}

	if response.Status != consensus_pb2.ConsensusFailBlockResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return
	}
}

// ---------------- Queries ------------------------

func (zs *ZmqService) GetBlocks(blockIds [][]byte) []*consensus_pb2.ConsensusBlock {
	request := &consensus_pb2.ConsensusBlocksGetRequest{
		BlockIds: blockIds,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_BLOCKS_GET_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusBlocksGetResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusBlocksGetResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown block: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusBlocksGetResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.Blocks
}

func (zs *ZmqService) GetChainHead() *consensus_pb2.ConsensusBlock {
	request := &consensus_pb2.ConsensusChainHeadGetRequest{}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_CHAIN_HEAD_GET_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusChainHeadGetResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusChainHeadGetResponse_NO_CHAIN_HEAD {
		log.Printf("could not initialize block, no chain head: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusChainHeadGetResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.Block
}

func (zs *ZmqService) GetSettings(blockId []byte, settings []string) []*consensus_pb2.ConsensusSettingsEntry {
	request := &consensus_pb2.ConsensusSettingsGetRequest{
		BlockId: blockId,
		Keys:    settings,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_SETTINGS_GET_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusSettingsGetResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusSettingsGetResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown block: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusSettingsGetResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.Entries
}

func (zs *ZmqService) GetState(blockId []byte, addresses []string) []*consensus_pb2.ConsensusStateEntry {
	request := &consensus_pb2.ConsensusStateGetRequest{
		BlockId:   blockId,
		Addresses: addresses,
	}

	requestBytes, err := proto.Marshal(request)
	if err != nil {
		log.Printf("could not marshal request: %v", err)
		return nil
	}

	corId, err := zs.connection.SendNewMsg(validator_pb2.Message_CONSENSUS_STATE_GET_REQUEST, requestBytes)
	if err != nil {
		log.Printf("could not send msg: %v", err)
		return nil
	}

	// Waiting for the answer
	_, msg, err := zs.connection.RecvMsgWithId(corId)
	if err != nil {
		log.Printf("could not receive msg response: %v", err)
		return nil
	}

	var response consensus_pb2.ConsensusStateGetResponse
	err = proto.Unmarshal(msg.Content, &response)
	if err != nil {
		log.Printf("could not unmarshal response: %v", err)
		return nil
	}

	if response.Status == consensus_pb2.ConsensusStateGetResponse_UNKNOWN_BLOCK {
		log.Printf("could not initialize block, unknown block: %v", msg.MessageType)
		return nil
	}

	if response.Status != consensus_pb2.ConsensusStateGetResponse_OK {
		log.Printf("could not initialize block: %v", msg.MessageType)
		return nil
	}

	return response.Entries
}
