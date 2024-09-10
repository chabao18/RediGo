package tcp

import (
	"RediGo/interface/tcp"
	"RediGo/lib/logger"
	"context"
	"net"
)

type Config struct {
	// tcp params config
	Address string
}

func ListenAndServeWithSignal(
	cfg *Config,
	handler tcp.Handler) error {

	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen" + cfg.Address)
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(
	listener net.Listener,
	handler tcp.Handler,
	closeChan <-chan struct{}) {

	ctx := context.Background()
	for true {
		conn, err := listener.Accept()
		if err != nil {
			break
		}

		logger.Info("accepted link")
		go func() {
			handler.Handle(ctx, conn)
		}()
	}
}
