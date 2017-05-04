package userip

import (
	"net/http"
	"net"
	"fmt"
	"context"
)

// FromRequest extracts the user ip address from req, if present.
func FromRequest(req *http.Request) (net.IP, error) {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("userip: %s is not ip:port", req.RemoteAddr)
	}

	userIP := net.ParseIP(ip)
	if userIP == nil {
		return nil, fmt.Errorf("userip: Failed to parse ip[%s]", ip)
	}

	return userIP, nil
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type key int

// userIPKey is the context key for the user ip address, its value of zero is
// arbitrary. If this package defined other context keys, they would have
// different integer value.
const userIPKey key = 0

// NewContext returns a new Context carrying userIP
func NewContext(ctx context.Context, userIP net.IP) context.Context {
	return context.WithValue(ctx, userIPKey, userIP)
}

func FromContext(ctx context.Context) (net.IP, bool) {
	// ctx.Value returns nil if ctx has no value for the key;
    // the net.IP type assertion returns ok=false for nil.

	userIP, ok := ctx.Value(userIPKey).(net.IP)

	return userIP, ok
}
