//go:build !linux

package fastpacketconn

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// ListenPacket sets up a UDP listener on the specified network and address.
// It returns a PacketConn that provides batched I/O operations.
func ListenPacket(ctx context.Context, network, address string) (*PacketConn, error) {
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", address, err)
	}

	udpConn, err := (&net.ListenConfig{}).ListenPacket(ctx, network, addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr.String(), err)
	}

	if addr.IP.To4() != nil {
		return NewPacketConn(ipv4.NewPacketConn(udpConn.(*net.UDPConn))), nil
	} else {
		return NewPacketConn(ipv6.NewPacketConn(udpConn.(*net.UDPConn))), nil
	}
}

// DialContext establishes a UDP connection to the specified address.
// It returns a PacketConn that provides batched I/O operations.
func DialContext(ctx context.Context, network, address string) (*PacketConn, error) {
	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", address, err)
	}

	udpConn, err := (&net.Dialer{}).DialContext(ctx, network, addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr.String(), err)
	}

	if addr.IP.To4() != nil {
		return NewPacketConn(ipv4.NewPacketConn(udpConn.(*net.UDPConn))), nil
	} else {
		return NewPacketConn(ipv6.NewPacketConn(udpConn.(*net.UDPConn))), nil
	}
}

const (
	batchSize = 64
)

var (
	_ batchPacketConn = (*ipv4.PacketConn)(nil)
	_ batchPacketConn = (*ipv6.PacketConn)(nil)
	_ *ipv4.Message   = (*ipv6.Message)(nil)
)

type batchPacketConn interface {
	io.Closer
	ReadBatch([]ipv6.Message, int) (int, error)
	WriteBatch([]ipv6.Message, int) (int, error)
}

// PacketConn is a wrapper around x/net/ipv4.PacketConn or x/net/ipv6.PacketConn
// that provides methods for reading and writing batches of UDP messages
type PacketConn struct {
	conn     batchPacketConn
	msgsPool sync.Pool
}

// NewPacketConn creates a new PacketConn instance wrapping the provided
// x/net/ipv4.PacketConn or x/net/ipv6.PacketConn.
func NewPacketConn(conn batchPacketConn) *PacketConn {
	return &PacketConn{
		conn: conn,
		msgsPool: sync.Pool{
			New: func() any {
				msgs := make([]ipv6.Message, batchSize)
				for i := range msgs {
					msgs[i].Buffers = make(net.Buffers, 1)
					msgs[i].Buffers[0] = make([]byte, math.MaxUint16)
				}
				return &msgs
			},
		},
	}
}

// ReadBatch reads multiple UDP segments from the underlying connection into the provided buffers.
// It fills each buffer with the received segment data, records the number of bytes read in 'sizes',
// and stores the source address of each segment in 'addrs'.
//
// Parameters:
// - bufs: A slice of byte slices, each representing a buffer to store incoming segment data.
// - sizes: A slice to record the number of bytes read into each buffer.
// - addrs: A slice to store the source address (net.UDPAddr) of each received segment.
//
// Returns:
// - The number of segments successfully read.
// - An error if any issue occurs during reading.
func (pc *PacketConn) ReadBatch(bufs [][]byte, sizes []int, addrs []*net.UDPAddr) (int, error) {
	msgs := pc.msgsPool.Get().(*[]ipv6.Message)
	defer pc.msgsPool.Put(msgs)

	for i := range *msgs {
		(*msgs)[i].Buffers[0] = bufs[i]
		(*msgs)[i].Addr = addrs[i]
	}

	n, err := pc.conn.ReadBatch(*msgs, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to read batch: %w", err)
	}

	for i := 0; i < n; i++ {
		sizes[i] = (*msgs)[i].N
	}

	return n, nil
}

// WriteBatch sends multiple UDP segments from the provided buffers to the specified destination addresses.
// It sends each buffer's contents to the corresponding address in 'addrs'.
//
// Parameters:
// - bufs: A slice of byte slices, each containing the data to send.
// - sizes: A slice indicating the number of bytes from each buffer to send.
// - addrs: A slice of destination addresses (net.UDPAddr) to which each segment should be sent.
//
// Returns:
// - The number of segments successfully sent.
// - An error if any issue occurs during writing.
func (pc *PacketConn) WriteBatch(bufs [][]byte, sizes []int, addrs []*net.UDPAddr) (int, error) {
	msgs := pc.msgsPool.Get().(*[]ipv6.Message)
	defer pc.msgsPool.Put(msgs)

	for i := range *msgs {
		(*msgs)[i].Buffers[0] = bufs[i][:sizes[i]]
		(*msgs)[i].Addr = addrs[i]
	}

	n, err := pc.conn.WriteBatch(*msgs, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return n, nil
}

// BatchSize is the maximum number of segments to read or write in a single batch operation.
func (pc *PacketConn) BatchSize() int {
	return batchSize
}

// Close closes the underlying connection of the PacketConn.
func (pc *PacketConn) Close() error {
	return pc.conn.Close()
}
