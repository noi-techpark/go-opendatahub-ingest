package dto

import "time"

type RawAny Raw[any, any]

type Raw[Rawtype any, Metatype any] struct {
	Provider  string    `json:"provider"`
	Timestamp time.Time `json:"timestamp"`
	Rawdata   Rawtype   `json:"rawdata"`
	Meta      Metatype  `json:"metadata,omitempty"`
}
