package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(cfg *Config,
	handler tcp.Handler) error {

	listen, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		logger.Error(err)
		return err
	}
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
	    sig := <- sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan<- struct{}{}
		}
	}()
	logger.Info("start listen...")
	ListenAndServe(listen, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener,
	handler tcp.Handler, closeChan <- chan struct{})  {

	ctx := context.Background()
	var waitDone sync.WaitGroup

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	go func() {
		<- closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()
	for true {
		conn, err := listener.Accept()
		if err != nil {
			logger.Fatal(err)
			break
		}
		logger.Info("accepted link")
		waitDone.Add(1)
		go func() {
			defer waitDone.Done()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
