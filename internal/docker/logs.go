package docker

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	defaultLogTail  = 100
	maxLogTail      = 10000
	defaultLogBytes = 1 << 20
	maxLogBytes     = 8 << 20

	// Docker multiplexes stdout and stderr of containers without a TTY into one
	// stream of frames: 1 byte stream type, 3 zero bytes, 4 bytes big-endian
	// length, then the payload.
	frameHeaderLen = 8
	streamStdout   = 1
	streamStderr   = 2
)

func (o LogOptions) tail() int {
	switch {
	case o.Tail <= 0:
		return defaultLogTail
	case o.Tail > maxLogTail:
		return maxLogTail
	}
	return o.Tail
}

func (o LogOptions) maxBytes() int {
	switch {
	case o.MaxBytes <= 0:
		return defaultLogBytes
	case o.MaxBytes > maxLogBytes:
		return maxLogBytes
	}
	return o.MaxBytes
}

// demux reads Docker's multiplexed log stream and returns at most maxBytes of
// payload, in order. Longer output sets Truncated; the rest is not read, so a
// huge log cannot exhaust memory.
func demux(r io.Reader, maxBytes int) (Logs, error) {
	var out Logs
	remaining := maxBytes
	header := make([]byte, frameHeaderLen)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF {
				return out, nil
			}
			return out, fmt.Errorf("read log frame header: %w", err)
		}
		var stream string
		switch header[0] {
		case streamStdout:
			stream = "stdout"
		case streamStderr:
			stream = "stderr"
		default:
			return out, fmt.Errorf("unexpected log stream type %d (is the container using a TTY?)", header[0])
		}
		size := int64(binary.BigEndian.Uint32(header[4:]))
		take := size
		if take > int64(remaining) {
			take = int64(remaining)
		}
		payload := make([]byte, take)
		if _, err := io.ReadFull(r, payload); err != nil {
			return out, fmt.Errorf("read log frame: %w", err)
		}
		if len(payload) > 0 {
			out.Chunks = append(out.Chunks, LogChunk{Stream: stream, Text: string(payload)})
		}
		remaining -= int(take)
		if take < size || remaining == 0 {
			// Anything further is dropped; report it only if there was more.
			if take < size {
				out.Truncated = true
			} else if more := moreData(r); more {
				out.Truncated = true
			}
			return out, nil
		}
	}
}

// moreData reports whether the stream has another byte, consuming at most one.
func moreData(r io.Reader) bool {
	var b [1]byte
	n, _ := r.Read(b[:])
	return n > 0
}
