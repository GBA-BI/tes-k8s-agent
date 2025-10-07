package utils

// Point convert a variable to its point
func Point[T any](i T) *T {
	return &i
}
