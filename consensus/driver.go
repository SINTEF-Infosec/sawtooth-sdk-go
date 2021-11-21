package consensus

type Driver interface {
	Start(endpoint string)
	Stop()
}
