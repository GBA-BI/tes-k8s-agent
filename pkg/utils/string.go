package utils

import (
	"fmt"
	"strings"
)

// Float2String ...
func Float2String(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", f), "0"), ".")
}
