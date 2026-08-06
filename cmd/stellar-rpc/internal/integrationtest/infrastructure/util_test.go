package infrastructure

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetFreeTCPPorts(t *testing.T) {
	ports := getFreeTCPPorts(t, 5)
	require.Len(t, ports, 5)
	seen := make(map[uint16]bool)
	for _, port := range ports {
		require.GreaterOrEqual(t, port, uint16(reservedPortRangeStart))
		require.Less(t, port, uint16(reservedPortRangeEnd))
		require.False(t, seen[port], "port %d handed out twice", port)
		seen[port] = true
	}
}
