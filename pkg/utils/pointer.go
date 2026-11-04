package utils

// ToPtr returns a pointer to the given value.
func ToPtr[T any](v T) *T {
	return &v
}

// ValueOrDefault returns the value of ptr if non-nil, otherwise defaultVal.
func ValueOrDefault[T any](ptr *T, defaultVal T) T {
	if ptr == nil {
		return defaultVal
	}
	return *ptr
}

// StringPtr returns a pointer to the string.
func StringPtr(s string) *string {
	return &s
}

// IntPtr returns a pointer to the int.
func IntPtr(i int) *int {
	return &i
}

// BoolPtr returns a pointer to the bool.
func BoolPtr(b bool) *bool {
	return &b
}
