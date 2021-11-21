package consensus

type InvalidState string

func (e *InvalidState) Error() string {
	return "invalid state"
}

type NoChainHead string

func (e *NoChainHead) Error() string {
	return "no chain head"
}

type ReceiveError string

func (e *ReceiveError) Error() string {
	return "receive error"
}

type UnknownBlock string

func (e *UnknownBlock) Error() string {
	return "unknown block"
}

type UnknownPeer string

func (e *UnknownPeer) Error() string {
	return "unknown peer"
}

type BlockNotReady string

func (e *BlockNotReady) Error() string {
	return "block not ready"
}
