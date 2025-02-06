// SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: MPL-2.0

package raw_data_bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/noi-techpark/go-opendatahub-ingest/dto"
	"github.com/noi-techpark/go-opendatahub-ingest/urn"
)

var (
	ErrDocumentNotFound = errors.New("document not found")
	ErrBadURN           = errors.New("bad urn format")
)

type Env struct {
	RAW_DATA_BRIDGE_ENDPOINT string
}

type RDBridge struct {
	endpoint string
}

func NewRDBridge(config Env) *RDBridge {
	return &RDBridge{
		endpoint: strings.TrimRight(config.RAW_DATA_BRIDGE_ENDPOINT, "/"),
	}
}

func (b RDBridge) Get(urn *urn.URN) ([]byte, error) {
	resp, err := http.Get(fmt.Sprintf("%s/urns/%s", b.endpoint, urn.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to get raw data: %s", err.Error())
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrDocumentNotFound
	}
	if resp.StatusCode == http.StatusBadRequest {
		return nil, ErrBadURN
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read raw data body: %s", err.Error())
	}
	return body, nil
}

func Get[P any](rdb *RDBridge, urn *urn.URN) (dto.Raw[P], error) {
	body, err := rdb.Get(urn)
	if err != nil {
		return dto.Raw[P]{}, err
	}
	// First, unmarshal into a temporary structure where Rawdata is a string
	var temp dto.Raw[string]
	err = json.Unmarshal(body, &temp)
	if err != nil {
		return dto.Raw[P]{}, fmt.Errorf("failed to unmarshal raw data wrapper: %s", err.Error())
	}

	// Now, unmarshal Rawdata (which is a string) into the desired type P
	var payload P
	err = json.Unmarshal([]byte(temp.Rawdata), &payload)
	if err != nil {
		return dto.Raw[P]{}, fmt.Errorf("failed to unmarshal rawdata field: %s", err.Error())
	}

	// Return properly typed dto.Raw
	return dto.Raw[P]{
		Provider:  temp.Provider,
		Timestamp: temp.Timestamp,
		Rawdata:   payload,
	}, nil
}
