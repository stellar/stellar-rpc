package preflight

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// A frame is a 4-byte big-endian length followed by that many bytes, which
// are JSON except in the frame that carries a response's body.

const (
	// maxFrameBytes bounds every frame but a response's body.
	maxFrameBytes = 4 << 20
	// maxResponseBytes leaves room for the response's fields around the JSON
	// that maxSimulationJSONBytes bounds.
	maxResponseBytes = maxSimulationJSONBytes + 1<<20
)

var (
	errFrameTooLarge = errors.New("frame too large")
	errInvalidFrame  = errors.New("invalid frame")
)

func writeFrame(w io.Writer, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeRawFrame(w, payload)
}

func writeRawFrame(w io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload))) //nolint:gosec // frames are far below 4 GiB
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// readFrame decodes the next frame into v, as readRawFrame reads it.
func readFrame(r io.Reader, maxBytes int, v any) error {
	payload, err := readRawFrame(r, maxBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("%w: %w", errInvalidFrame, err)
	}
	return nil
}

// readRawFrame returns the next frame's payload. It returns io.EOF only when
// r ends before the frame starts, and errFrameTooLarge, without reading the
// payload, when it is longer than maxBytes.
func readRawFrame(r io.Reader, maxBytes int) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if int64(n) > int64(maxBytes) {
		return nil, fmt.Errorf("%w: %d bytes, over the %d-byte limit", errFrameTooLarge, n, maxBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}
