package tcp

import (
	"RediGo/interface/tcp"
	"RediGo/lib/logger"
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Config holds the configuration parameters for TCP server.
type Config struct {
	// Address is the TCP address the server listens on.
	Address string
}

// ListenAndServeWithSignal starts a TCP server and listens for system signals to gracefully shut down.
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)

	// Register signal notifications for graceful shutdown.
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()

	// Start listening on the specified TCP address.
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listening on " + cfg.Address)

	// Start the server with the specified handler and close channel.
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe handles incoming TCP connections and shuts down gracefully on receiving a signal.
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	go func() {
		<-closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// Ensure listener and handler are closed when function exits.
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}

		logger.Info("accepted connection")
		waitDone.Add(1)
		go func() {
			defer waitDone.Done()
			handler.Handle(ctx, conn)
		}()
	}

	waitDone.Wait()
}
