package qmanutils

func Map[T1, T2 any](f func(T1) T2, arr []T1) []T2 {
	var result []T2
	for _, v := range arr {
		result = append(result, f(v))
	}
	return result
}
