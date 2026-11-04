package types

// PaginationQuery represents standard pagination query parameters.
type PaginationQuery struct {
	Page  int `json:"page"`
	Limit int `json:"limit"`
}

// PaginatedResult wraps a slice of items with pagination metadata.
type PaginatedResult[T any] struct {
	Items      []T `json:"items"`
	Total      int `json:"total"`
	Page       int `json:"page"`
	Limit      int `json:"limit"`
	TotalPages int `json:"total_pages"`
}

// NewPaginatedResult creates a PaginatedResult calculating TotalPages.
func NewPaginatedResult[T any](items []T, total, page, limit int) PaginatedResult[T] {
	totalPages := 0
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}
	return PaginatedResult[T]{
		Items:      items,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	}
}
