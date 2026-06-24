package model

type ModelMapping struct {
	ID          int    `json:"id" gorm:"primaryKey"`
	RequestName string `json:"request_name" gorm:"uniqueIndex;not null"`
	ActualName  string `json:"actual_name" gorm:"not null"`
	Enabled     bool   `json:"enabled" gorm:"default:true"`
}

type ModelMappingCreateRequest struct {
	RequestName string `json:"request_name" binding:"required"`
	ActualName  string `json:"actual_name" binding:"required"`
	Enabled     *bool  `json:"enabled"`
}

type ModelMappingUpdateRequest struct {
	ID          int    `json:"id" binding:"required"`
	RequestName string `json:"request_name" binding:"required"`
	ActualName  string `json:"actual_name" binding:"required"`
	Enabled     *bool  `json:"enabled"`
}
