// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package fastpacketconn_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dpeckett/fastpacketconn"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

func TestPacketConnThroughput(t *testing.T) {
	const (
		duration   = 5 * time.Second
		MTU        = 1500
		packetSize = 1200
	)

	g, ctx := errgroup.WithContext(t.Context())

	var totalBytesSent, totalBytesReceived int64

	startTime := time.Now()

	g.Go(func() error {
		pc, err := fastpacketconn.ListenPacket(ctx, "udp", ":1234")
		if err != nil {
			return fmt.Errorf("failed to listen for packets")
		}

		g.Go(func() error {
			<-ctx.Done()
			return pc.Close()
		})

		batchSize := pc.BatchSize()
		bufs := make([][]byte, batchSize)
		sizes := make([]int, batchSize)
		addrs := make([]*net.UDPAddr, batchSize)

		for i := range bufs {
			bufs[i] = make([]byte, MTU)
		}

		for time.Since(startTime) < duration {
			nMsg, err := pc.ReadBatch(bufs, sizes, addrs)
			if err != nil {
				return err
			}
			for i := 0; i < nMsg; i++ {
				totalBytesReceived += int64(sizes[i])
			}
		}

		return nil
	})

	g.Go(func() error {
		time.Sleep(500 * time.Millisecond) // Wait for listener

		pc, err := fastpacketconn.DialContext(ctx, "udp", ":1234")
		if err != nil {
			return fmt.Errorf("failed to create packet conn")
		}

		g.Go(func() error {
			<-ctx.Done()
			return pc.Close()
		})

		batchSize := pc.BatchSize()
		bufs := make([][]byte, batchSize)
		sizes := make([]int, batchSize)
		addrs := make([]*net.UDPAddr, batchSize)

		for i := range bufs {
			bufs[i] = make([]byte, MTU)
			_, _ = rand.Read(bufs[i])
			addrs[i] = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
		}

		for time.Since(startTime) < duration {
			for i := range bufs {
				sizes[i] = packetSize
				bufs[i] = bufs[i][:sizes[i]]
			}

			n, err := pc.WriteBatch(bufs, sizes, addrs)
			if err != nil {
				return err
			}
			for i := 0; i < n; i++ {
				totalBytesSent += int64(sizes[i])
			}
		}

		return context.Canceled // Signal completion
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		require.NoError(t, err)
	}

	elapsed := time.Since(startTime).Seconds()

	fmt.Printf("Send throughput: %.2f Mbps\n", float64(totalBytesSent*8)/elapsed/1_000_000)
	fmt.Printf("Recv throughput: %.2f Mbps\n", float64(totalBytesReceived*8)/elapsed/1_000_000)
}
