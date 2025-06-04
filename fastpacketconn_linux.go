//go:build linux

// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fastpacketconn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"syscall"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"
)

// ListenPacket sets up a UDP listener on the specified network and address with
// Generic Receive Offload (GRO) and Generic Segmentation Offload (GSO) enabled.
func ListenPacket(ctx context.Context, network, address string) (*PacketConn, error) {
	listenConfig := &net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			// Enable UDP GRO (Generic Receive Offload) for the listener
			var innerErr error
			err := c.Control(func(fd uintptr) {
				innerErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_UDP, unix.UDP_GRO, 1)
			})
			err = errors.Join(err, innerErr)
			if err != nil {
				return fmt.Errorf("failed to set UDP_GRO option: %w", err)
			}

			return nil
		},
	}

	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", address, err)
	}

	udpConn, err := listenConfig.ListenPacket(ctx, network, addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr.String(), err)
	}

	if err := udpConn.(*net.UDPConn).SetReadBuffer(socketBufferSize); err != nil {
		return nil, fmt.Errorf("failed to set read buffer size: %w", err)
	}

	if err := udpConn.(*net.UDPConn).SetWriteBuffer(socketBufferSize); err != nil {
		return nil, fmt.Errorf("failed to set write buffer size: %w", err)
	}

	if addr.IP.To4() != nil {
		return NewPacketConn(ipv4.NewPacketConn(udpConn.(*net.UDPConn))), nil
	} else {
		return NewPacketConn(ipv6.NewPacketConn(udpConn.(*net.UDPConn))), nil
	}
}

// DialContext establishes a UDP connection to the specified address with
// Generic Receive Offload (GRO) and Generic Segmentation Offload (GSO) enabled.
func DialContext(ctx context.Context, network, address string) (*PacketConn, error) {
	dialerConfig := &net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			var innerErr error
			err := c.Control(func(fd uintptr) {
				innerErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_UDP, unix.UDP_GRO, 1)
			})
			err = errors.Join(err, innerErr)
			if err != nil {
				return fmt.Errorf("failed to set UDP_GRO option: %w", err)
			}

			return nil
		},
	}

	addr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address %s: %w", address, err)
	}

	udpConn, err := dialerConfig.DialContext(ctx, network, addr.String())
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr.String(), err)
	}

	if err := udpConn.(*net.UDPConn).SetReadBuffer(socketBufferSize); err != nil {
		return nil, fmt.Errorf("failed to set read buffer size: %w", err)
	}

	if err := udpConn.(*net.UDPConn).SetWriteBuffer(socketBufferSize); err != nil {
		return nil, fmt.Errorf("failed to set write buffer size: %w", err)
	}

	if addr.IP.To4() != nil {
		return NewPacketConn(ipv4.NewPacketConn(udpConn.(*net.UDPConn))), nil
	} else {
		return NewPacketConn(ipv6.NewPacketConn(udpConn.(*net.UDPConn))), nil
	}
}

const (
	socketBufferSize = (1 << 20) * 7 // 7 MB
	batchSize        = 128
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
// that provides methods for reading and writing batches of UDP messages with
// Generic Receive Offload (GRO) and Generic Segmentation Offload (GSO) support.
type PacketConn struct {
	conn     batchPacketConn
	bufsPool sync.Pool
	msgsPool sync.Pool
}

// NewPacketConn creates a new PacketConn instance wrapping the provided
// x/net/ipv4.PacketConn or x/net/ipv6.PacketConn.
func NewPacketConn(conn batchPacketConn) *PacketConn {
	return &PacketConn{
		conn: conn,
		bufsPool: sync.Pool{
			New: func() any {
				buf := make([]byte, math.MaxUint16)
				return &buf
			},
		},
		msgsPool: sync.Pool{
			New: func() any {
				msgs := make([]ipv6.Message, batchSize)
				for i := range msgs {
					msgs[i].Buffers = make(net.Buffers, gsoMaxSegments)
					msgs[i].OOB = make([]byte, unix.CmsgSpace(sizeOfGSOData))
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

	// Prevent overflowing the supplied buffers.
	// This is super conservative, maybe we can ask the user to provide hints?
	maxMessages := len(bufs) / gsoMaxSegments

	*msgs = (*msgs)[:maxMessages]
	for i := range *msgs {
		buf := pc.bufsPool.Get().(*[]byte)
		defer pc.bufsPool.Put(buf)

		(*msgs)[i].Buffers = (*msgs)[i].Buffers[:1]
		(*msgs)[i].Buffers[0] = (*buf)[:cap(*buf)]
	}

	nMsgs, err := pc.conn.ReadBatch(*msgs, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to read batch: %w", err)
	}

	return splitCoalescedMessages((*msgs)[:nMsgs], bufs, sizes, addrs)
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

	*msgs = (*msgs)[:cap(*msgs)]
	for i := range *msgs {
		(*msgs)[i].Buffers = (*msgs)[i].Buffers[:cap((*msgs)[i].Buffers)]
	}

	nMsgs := coalesceMessages(*msgs, bufs, sizes, addrs)
	(*msgs) = (*msgs)[:nMsgs]

	offset := 0
	for offset < nMsgs {
		written, err := pc.conn.WriteBatch((*msgs)[offset:], 0)
		if err != nil {
			var serr *os.SyscallError
			if errors.As(err, &serr) {
				if serr.Err == unix.EIO {
					// EIO is returned by udp_send_skb() if the device driver does not have
					// tx checksumming enabled, which is a hard requirement of UDP_SEGMENT.
					err = errors.New("device driver does not support tx checksumming")
				}
			}
			return -1, fmt.Errorf("failed to write batch: %w", err)
		}
		offset += written
	}

	return len(bufs), nil
}

// BatchSize is the maximum number of segments to read or write in a single batch operation.
func (pc *PacketConn) BatchSize() int {
	return batchSize
}

// Close closes the underlying connection of the PacketConn.
func (pc *PacketConn) Close() error {
	return pc.conn.Close()
}

const (
	// Each GSO control message contains a 2-byte size field.
	sizeOfGSOData = 2
	// This is a hard limit imposed by the kernel.
	gsoMaxSegments  = 64
	maxDatagramSize = 65507
)

func coalesceMessages(msgs []ipv6.Message, bufs [][]byte, sizes []int, addrs []*net.UDPAddr) int {
	n := 0
	i := 0

	for i < len(bufs) {
		start := i
		end := i + 1
		totalSize := sizes[start]

		// Group up to gsoMaxSegments with the same address and size, keeping total under maxDatagramSize
		for end < len(bufs) &&
			end-start < gsoMaxSegments &&
			addrs[end].IP.Equal(addrs[start].IP) &&
			addrs[end].Port == addrs[start].Port &&
			sizes[end] == sizes[start] &&
			totalSize+sizes[end] <= maxDatagramSize {
			totalSize += sizes[end]
			end++
		}

		// Construct a coalesced message from bufs[start:end]
		msg := &msgs[n]
		msg.Buffers = msg.Buffers[:end-start]
		for j := range msg.Buffers {
			msg.Buffers[j] = bufs[start+j][:sizes[start+j]]
		}
		msg.Addr = addrs[start]
		msg.OOB = msg.OOB[:cap(msg.OOB)]
		msg.N = 0

		if end-start > 1 {
			// Set GSO size if more than one segment
			setLen := setGSOSize(msg.OOB, uint16(sizes[start]))
			msg.OOB = msg.OOB[:setLen]
		} else {
			msg.OOB = msg.OOB[:0]
		}

		n++
		i = end
	}

	return n
}

func splitCoalescedMessages(msgs []ipv6.Message, bufs [][]byte, sizes []int, addrs []*net.UDPAddr) (int, error) {
	n := 0

	for _, msg := range msgs {
		groSize, err := getGSOSize(msg.OOB[:msg.NN])
		if err != nil {
			return n, err
		}

		data := msg.Buffers[0][:msg.N]
		total := len(data)

		if groSize == 0 {
			if n >= len(bufs) {
				return n, errors.New("too many segments")
			}
			sizes[n] = copy(bufs[n], data)
			if udpAddr, ok := msg.Addr.(*net.UDPAddr); ok {
				addrs[n] = udpAddr
			}
			n++
			continue
		}

		for offset := 0; offset < total; offset += groSize {
			if n >= len(bufs) {
				return n, errors.New("too many segments")
			}
			end := offset + groSize
			if end > total {
				end = total
			}
			sizes[n] = copy(bufs[n], data[offset:end])
			if udpAddr, ok := msg.Addr.(*net.UDPAddr); ok {
				addrs[n] = udpAddr
			}
			n++
		}
	}

	return n, nil
}

func setGSOSize(control []byte, gsoSize uint16) int {
	space := unix.CmsgSpace(sizeOfGSOData)
	control = control[:space]
	binary.NativeEndian.PutUint64(control[0:8], uint64(unix.CmsgLen(sizeOfGSOData))) // Length
	binary.NativeEndian.PutUint32(control[8:12], unix.IPPROTO_UDP)                   // Level
	binary.NativeEndian.PutUint32(control[12:16], unix.UDP_SEGMENT)                  // Type
	binary.NativeEndian.PutUint16(control[16:], gsoSize)                             // GSO size
	return space
}

func getGSOSize(control []byte) (int, error) {
	rem := control
	for len(rem) > unix.SizeofCmsghdr {
		hdr, data, nextRem, err := unix.ParseOneSocketControlMessage(rem)
		if err != nil {
			return 0, fmt.Errorf("error parsing socket control message: %w", err)
		}
		rem = nextRem
		if hdr.Level == unix.IPPROTO_UDP && hdr.Type == unix.UDP_GRO && len(data) >= sizeOfGSOData {
			if sizeOfGSOData != 2 {
				return 0, fmt.Errorf("unexpected GSO data size: %d", sizeOfGSOData)
			}
			gso := binary.NativeEndian.Uint16(data[:2])
			return int(gso), nil
		}
	}
	return 0, nil
}
