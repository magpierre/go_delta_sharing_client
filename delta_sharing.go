// Copyright 2025 Magnus Pierre
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delta_sharing

import (
	"context"
	"errors"

	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

func parseURL(url string) (string, string, string, string) {
	i := strings.LastIndex(url, "#")
	if i < 0 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	profile := url[0:i]
	fragments := strings.Split(url[i+1:], ".")
	if len(fragments) != 3 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	share := strings.Trim(fragments[0], " ")
	schema := strings.Trim(fragments[1], " ")
	table := strings.Trim(fragments[2], " ")
	if len(share) == 0 || len(schema) == 0 || len(table) == 0 {
		fmt.Println("Invalid URL:", url)
		return "", "", "", ""
	}
	return profile, share, schema, table
}

func LoadAsArrowTable(ctx context.Context, url string, fileno int) (arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadAsArrowTable"
	profile, shareStr, schemaStr, tableStr := parseURL(url)
	s, err := NewSharingClient(profile)
	if err != nil {
		return nil, err
	}
	t := Table{Share: shareStr, Schema: schemaStr, Name: tableStr}
	lf, err := s.ListFilesInTable(ctx, t)
	if err != nil {
		return nil, err
	}

	if fileno > len(lf.AddFiles) || fileno < 0 {
		return nil, errors.New("invalid index")
	}
	pf, err := s.ReadFileUrlToArrowTable(ctx, lf.AddFiles[fileno].Url)
	if err != nil {
		return nil, &DSErr{pkg, fn, "pqarrow.ReadTable", err.Error()}
	}

	return pf, err
}

func LoadArrowTable(ctx context.Context, client SharingClient, table Table, fileId string) (arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadArrowTable"
	if client == nil {
		return nil, &DSErr{pkg, fn, "client == nil", "Invalid client"}
	}

	f, err := client.ListFilesInTable(ctx, table)
	if err != nil {
		return nil, err
	}
	var urlValue *string

	for _, v := range f.AddFiles {
		if v.Id == fileId {
			urlValue = &v.Url
			break
		}
	}

	if urlValue == nil {
		return nil, &DSErr{pkg, fn, "v.Id == fileId", "fileid not found in table"}
	}

	pf, err := client.ReadFileUrlToArrowTable(ctx, *urlValue)
	if err != nil {
		return nil, &DSErr{pkg, fn, "pqarrow.ReadTable", err.Error()}
	}
	return pf, err
}

type SharingClient interface {
	ListShares(ctx context.Context, maxResults int, pageToken string) ([]share, string, error)
	ListSchemas(ctx context.Context, share share, maxResults int, pageToken string) ([]schema, string, error)
	ListTables(ctx context.Context, schema schema, maxResults int, pageToken string) ([]Table, string, error)
	ListAllTables(ctx context.Context, maxResults int, pageToken string) ([]Table, string, error)
	ListFilesInTable(ctx context.Context, t Table) (*listFilesInTableResponse, error)
	GetTableVersion(ctx context.Context, t Table) (int64, error)
	GetTableMetadata(ctx context.Context, t Table) (*metadata, error)
	ListTableChanges(ctx context.Context, t Table, options CdfOptions) (*listCdfFilesResponse, error)
	ReadFileUrlToArrowTable(ctx context.Context, url string) (arrow.Table, error)
}
type sharingClient struct {
	restClient *deltaSharingRestClient
}

func NewSharingClient(ProfileFile string) (SharingClient, error) {
	pkg := "delta_sharing.go"
	fn := "NewSharingClient"
	p, err := newDeltaSharingProfile(ProfileFile)
	if err != nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingProfile", err.Error()}
	}
	r := newDeltaSharingRestClient(p, 5)
	if r == nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingRestClient", "Could not create DeltaSharingRestClient"}
	}
	return &sharingClient{restClient: r}, err
}

func NewSharingClientFromString(ProfileString string) (SharingClient, error) {
	pkg := "delta_sharing.go"
	fn := "NewSharingClientWithString"
	p, err := newDeltaSharingProfileFromString(ProfileString)
	if err != nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingProfileFromString", err.Error()}
	}
	r := newDeltaSharingRestClient(p, 5)
	if r == nil {
		return nil, &DSErr{pkg, fn, "NewDeltaSharingRestClient", "Could not create DeltaSharingRestClient"}
	}
	return &sharingClient{restClient: r}, err
}

func (s *sharingClient) ListShares(ctx context.Context, maxResults int, pageToken string) ([]share, string, error) {
	pkg := "delta_sharing.go"
	fn := "ListShares"
	sh, err := s.restClient.ListShares(ctx, maxResults, pageToken)
	if err != nil {
		return nil, "", &DSErr{pkg, fn, "s.RestClient.ListShares", err.Error()}
	}
	return sh.Shares, sh.NextPageToken, nil
}

func (s *sharingClient) ListSchemas(ctx context.Context, share share, maxResults int, pageToken string) ([]schema, string, error) {
	sc, err := s.restClient.ListSchemas(ctx, share, maxResults, pageToken)
	if err != nil {
		return nil, "", err
	}
	return sc.Schemas, sc.NextPageToken, nil
}

func (s *sharingClient) ListTables(ctx context.Context, schema schema, maxResults int, pageToken string) ([]Table, string, error) {
	t, err := s.restClient.ListTables(ctx, schema, maxResults, pageToken)
	if err != nil {
		return nil, "", err
	}
	return t.Tables, t.NextPageToken, nil
}

func (s *sharingClient) ListAllTables(ctx context.Context, maxResults int, pageToken string) ([]Table, string, error) {
	// Note: This fetches all tables across ALL shares
	// Pagination is complex here - we need to iterate through shares
	// For now, we'll collect all tables but won't support cross-share pagination
	// A better design would be to paginate at the share level or require a share parameter
	sh, err := s.restClient.ListShares(ctx, 0, "")
	if err != nil {
		return nil, "", err
	}
	var tl []Table
	var lastToken string
	for _, v := range sh.Shares {
		x, err := s.restClient.ListAllTables(ctx, v, maxResults, pageToken)
		if err != nil {
			return nil, "", err
		}
		tl = append(tl, x.Tables...)
		if x.NextPageToken != "" {
			lastToken = x.NextPageToken
		}
	}
	return tl, lastToken, nil
}

func (s *sharingClient) ListFilesInTable(ctx context.Context, t Table) (*listFilesInTableResponse, error) {
	return s.restClient.ListFilesInTable(ctx, t)
}

func (s *sharingClient) GetTableVersion(ctx context.Context, t Table) (int64, error) {
	v, err := s.restClient.QueryTableVersion(ctx, t)
	if err != nil {
		return -1, err
	}
	return v.DeltaTableVersion, nil
}

func (s *sharingClient) GetTableMetadata(ctx context.Context, t Table) (*metadata, error) {
	m, err := s.restClient.QueryTableMetadata(ctx, t)
	if err != nil {
		return nil, err
	}
	return &m.Metadata, nil
}

func (s *sharingClient) ListTableChanges(ctx context.Context, t Table, options CdfOptions) (*listCdfFilesResponse, error) {
	return s.restClient.ListTableChanges(ctx, t, options)
}

func (s *sharingClient) ReadFileUrlToArrowTable(ctx context.Context, url string) (arrow.Table, error) {
	return s.restClient.ReadFileUrlToArrowTable(ctx, url)
}
