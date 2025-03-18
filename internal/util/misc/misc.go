package misc

func TernaryIf[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}
