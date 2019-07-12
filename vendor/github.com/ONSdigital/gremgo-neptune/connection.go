package gremgo

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

//go:generate moq -out dialer_moq_test.go . dialer

type dialer interface {
	connect() error
	connectCtx(context.Context) error
	IsConnected() bool
	IsDisposed() bool
	write([]byte) error
	read() (int, []byte, error)
	readCtx(context.Context, chan message)
	close() error
	getAuth() *auth
	ping(errs chan error)
	pingCtx(context.Context, chan error)
}

/////
/*
WebSocket Connection
*/
/////

// Ws is the dialer for a WebSocket connection
type Ws struct {
	host         string
	conn         *websocket.Conn
	auth         *auth
	disposed     bool
	connected    bool
	pingInterval time.Duration
	writingWait  time.Duration
	readingWait  time.Duration
	timeout      time.Duration
	quit         chan struct{}
	sync.RWMutex
}

//Auth is the container for authentication data of dialer
type auth struct {
	username string
	password string
}

func (ws *Ws) connect() (err error) {
	return ws.connectCtx(context.Background())
}

func (ws *Ws) connectCtx(ctx context.Context) (err error) {
	d := websocket.Dialer{
		WriteBufferSize:  512 * 1024,
		ReadBufferSize:   512 * 1024,
		HandshakeTimeout: 5 * time.Second, // Timeout or else we'll hang forever and never fail on bad hosts.
	}
	ws.conn, _, err = d.DialContext(ctx, ws.host, http.Header{})
	if err != nil {
		return
	}
	ws.connected = true
	ws.conn.SetPongHandler(func(appData string) error {
		ws.Lock()
		ws.connected = true
		ws.Unlock()
		return nil
	})
	return
}

// IsConnected returns whether the underlying websocket is connected
func (ws *Ws) IsConnected() bool {
	return ws.connected
}

// IsDisposed returns whether the underlying websocket is disposed
func (ws *Ws) IsDisposed() bool {
	return ws.disposed
}

func (ws *Ws) write(msg []byte) (err error) {
	// XXX want to do locking here?
	// ws.RWMutex.Lock()
	// defer ws.RWMutex.Unlock()
	err = ws.conn.WriteMessage(websocket.BinaryMessage, msg)
	return
}

func (ws *Ws) read() (msgType int, msg []byte, err error) {
	// XXX want to do locking here?
	// ws.RWMutex.RLock()
	// defer ws.RWMutex.RUnlock()
	msgType, msg, err = ws.conn.ReadMessage()
	return
}

func (ws *Ws) readCtx(ctx context.Context, rxMsgChan chan message) {
	// XXX want to do locking here?
	// ws.RWMutex.RLock()
	// defer ws.RWMutex.RUnlock()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgType, msg, err := ws.conn.ReadMessage()
			rxMsgChan <- message{msgType, msg, err}
			if msgType == -1 {
				return
			}
		}
	}
}

func (ws *Ws) close() (err error) {
	defer func() {
		close(ws.quit)
		ws.conn.Close()
		ws.disposed = true
	}()

	// XXX want to do locking here?
	// ws.RWMutex.Lock()
	// defer ws.RWMutex.Unlock()
	err = ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")) //Cleanly close the connection with the server
	return
}

func (ws *Ws) getAuth() *auth {
	if ws.auth == nil {
		panic("You must create a Secure Dialer for authenticating with the server")
	}
	return ws.auth
}

func (ws *Ws) ping(errs chan error) {
	ws.pingCtx(context.Background(), errs)
}

func (ws *Ws) pingCtx(ctx context.Context, errs chan error) {
	ticker := time.NewTicker(ws.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			connected := true
			if err := ws.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(ws.writingWait)); err != nil {
				errs <- err
				connected = false
			}
			ws.Lock()
			ws.connected = connected
			ws.Unlock()
		case <-ctx.Done():
			return
		case <-ws.quit:
			return
		}
	}
}

// writeWorker works on a loop and dispatches messages as soon as it receives them
func (c *Client) writeWorker(errs chan error, quit chan struct{}) {
	for {
		select {
		case msg := <-c.requests:
			c.Lock()
			err := c.conn.write(msg)
			if err != nil {
				errs <- err
				c.Errored = true
				c.Unlock()
				break
			}
			c.Unlock()

		case <-quit:
			return
		}
	}
}

// writeWorkerCtx works on a loop and dispatches messages as soon as it receives them
func (c *Client) writeWorkerCtx(ctx context.Context, errs chan error) {
	for {
		select {
		case msg := <-c.requests:
			c.Lock()
			err := c.conn.write(msg)
			if err != nil {
				errs <- err
				c.Errored = true
				c.Unlock()
				break
			}
			c.Unlock()

		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) readWorker(errs chan error, quit chan struct{}) { // readWorker works on a loop and sorts messages as soon as it receives them
	for {
		msgType, msg, err := c.conn.read()
		if msgType == -1 { // msgType == -1 is noFrame (close connection)
			return
		}
		if err != nil {
			errs <- errors.Wrapf(err, "Receive message type: %d", msgType)
			c.Errored = true
			break
		}
		if msg != nil {
			if err = c.handleResponse(msg); err != nil {
				// XXX this makes the err fatal
				errs <- errors.Wrapf(err, "handleResponse fail: %q", msg)
				c.Errored = true
			}
		}

		select {
		case <-quit:
			return
		default:
			continue
		}
	}
}

type message struct {
	mType int
	msg   []byte
	err   error
}

// readWorkerCtx works on a loop and sorts read messages as soon as it receives them
func (c *Client) readWorkerCtx(ctx context.Context, msgs chan []byte, errs chan error) {
	receivedMsgChan := make(chan message, 100)
	go c.conn.readCtx(ctx, receivedMsgChan)

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case msg := <-receivedMsgChan:
			if msg.mType == -1 { // msgType == -1 is noFrame (close connection)
				return
			}
			if msg.err != nil {
				errs <- errors.Wrapf(msg.err, "Receive message type: %d", msg.mType)
				c.Errored = true
				return
			}
			if msg.msg != nil {
				msgs <- msg.msg
			}
		}
	}
}
