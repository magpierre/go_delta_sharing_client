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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

/* Response types */
type listSharesResponse struct {
	Shares        []share
	NextPageToken string `json:"nextPageToken,omitempty"`
}
type listSchemasResponse struct {
	Schemas       []schema
	NextPageToken string
}
type listTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type listAllTablesResponse struct {
	Tables        []Table
	NextPageToken string
}
type queryTableMetadataReponse struct {
	Protocol protocol
	Metadata metadata
}
type queryTableVersionResponse struct {
	DeltaTableVersion int64
}
type listFilesInTableResponse struct {
	Protocol protocol
	Metadata metadata
	AddFiles []File
}

type listCdfFilesResponse struct {
	Protocol protocol
	Metadata metadata
	Action   struct {
		Add    []File
		Cdf    []CDFFile
		Remove []RemoveFile
	}
}

type deltaSharingRestClient struct {
	profile    *deltaSharingProfile
	numRetries int
	//	cacheDir   string
	//	cache      afero.Fs
}

/* Constructor for the DeltaSharingRestClient */
func newDeltaSharingRestClient(profile *deltaSharingProfile, numRetries int) *deltaSharingRestClient {

	return &deltaSharingRestClient{
		profile:    profile,
		numRetries: numRetries,
		//		cache:      nil,
	}

}

func (d *deltaSharingRestClient) readFileReader(ctx context.Context, urlString string) (*bytes.Reader, error) {
	pkg := "rest_client.go"
	fn := "readFileReader"

	req, err := http.NewRequestWithContext(ctx, "GET", urlString, nil)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewRequestWithContext", err.Error()}
	}

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	var b bytes.Buffer
	_, err = io.Copy(&b, r.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.Copy", err.Error()}
	}
	br := bytes.NewReader(b.Bytes())
	return br, err
}

func (d *deltaSharingRestClient) callSharingServer(ctx context.Context, request string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServer"
	var responses [][]byte
	rawUrl := d.profile.Endpoint + request
	urlval, _ := url.Parse(rawUrl)

	req, err := http.NewRequestWithContext(ctx, "GET", urlval.String(), nil)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewRequestWithContext", err.Error()}
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Authorization", "Bearer "+d.profile.BearerToken)

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
	}
	defer response.Body.Close()

	var b bytes.Buffer
	_, err = io.Copy(&b, response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.Copy", err.Error()}
	}
	x := bytes.Split(b.Bytes(), []byte{'\n'})
	for _, v := range x {
		if len(v) > 0 {
			responses = append(responses, v)
		}
	}
	return &responses, err
}
func (d *deltaSharingRestClient) callSharingServerWithParameters(ctx context.Context, request string, maxResult int, pageToken string) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "callSharingServerWithParameters"
	var responses [][]byte

	// Parse base URL
	rawUrl := d.profile.Endpoint + request
	urlval, err := url.Parse(rawUrl)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}

	// Add query parameters
	query := urlval.Query()

	// Add maxResults if specified
	if maxResult > 0 {
		query.Add("maxResults", strconv.Itoa(maxResult))
	}

	// Add pageToken if specified
	if pageToken != "" {
		query.Add("pageToken", pageToken)
	}

	// Set the encoded query string
	urlval.RawQuery = query.Encode()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", urlval.String(), nil)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewRequestWithContext", err.Error()}
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Authorization", "Bearer "+d.profile.BearerToken)

	var response *http.Response
	var retryCnt = 0
	for {
		// Check if context is cancelled before retry
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= d.numRetries && d.shouldRetry(response) {
				retryCnt++
				continue
			}

			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}
	defer response.Body.Close()
	var b bytes.Buffer
	_, err = io.Copy(&b, response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.Copy", err.Error()}
	}
	x := bytes.Split(b.Bytes(), []byte{'\n'})
	for _, v := range x {
		if len(v) > 0 {
			responses = append(responses, v)
		}
	}

	return &responses, err
}

func (d *deltaSharingRestClient) getResponseHeader(ctx context.Context, request string) (map[string][]string, error) {
	pkg := "rest_client.go"
	fn := "getResponseHeader"
	urlParsed, err := url.Parse(d.profile.Endpoint + request)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, "HEAD", urlParsed.String(), nil)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewRequestWithContext", err.Error()}
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Authorization", "Bearer "+d.profile.BearerToken)

	var response *http.Response
	var retryCnt = 0
	for {
		// Check if context is cancelled before retry
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= d.numRetries && d.shouldRetry(response) {
				retryCnt++
				continue
			}
			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}
	return response.Header, err
}

func (c deltaSharingRestClient) ListShares(ctx context.Context, maxResult int, pageToken string) (*listSharesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListShares"
	url := "/shares"

	rd, err := c.callSharingServerWithParameters(ctx, url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var share protoShare
	err = json.Unmarshal((*rd)[0], &share)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	return &listSharesResponse{
		Shares:        share.Items,
		NextPageToken: share.NextPageToken,
	}, nil
}

func (c deltaSharingRestClient) ListSchemas(ctx context.Context, share share, maxResult int, pageToken string) (*listSchemasResponse, error) {
	pkg := "rest_client.go"
	fn := "ListSchemas"
	url := "/shares/" + share.Name + "/schemas"
	rd, err := c.callSharingServerWithParameters(ctx, url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "array returned is too short"}
	}
	var schema protoSchema
	err = json.Unmarshal((*rd)[0], &schema)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	return &listSchemasResponse{
		Schemas:       schema.Items,
		NextPageToken: schema.NextPageToken,
	}, nil
}

func (c deltaSharingRestClient) ListTables(ctx context.Context, schema schema, maxResult int, pageToken string) (*listTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListTables"
	url := "/shares/" + schema.Share + "/schemas/" + schema.Name + "/tables"
	rd, err := c.callSharingServerWithParameters(ctx, url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", "Invalid length of array"}
	}
	var tbl protoTable
	err = json.Unmarshal((*rd)[0], &tbl)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	return &listTablesResponse{
		Tables:        tbl.Items,
		NextPageToken: tbl.NextPageToken,
	}, nil
}

func (c deltaSharingRestClient) ListAllTables(ctx context.Context, share share, maxResult int, pageToken string) (*listAllTablesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListAllTables"
	url := "/shares/" + share.Name + "/all-tables"
	rd, err := c.callSharingServerWithParameters(ctx, url, maxResult, pageToken)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServerWithParameters", err.Error()}
	}
	if rd == nil || len(*rd) < 1 {
		return nil, &DSErr{pkg, fn, "len(*rd)", "array returned is too short"}
	}
	var tables []Table
	var table protoTable
	var nextPageToken string

	for _, v := range (*rd)[0:] {
		// Check context in loop
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		err = json.Unmarshal(v, &table)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		tables = append(tables, table.Items...)
		// Keep the last NextPageToken (from the final response line)
		if table.NextPageToken != "" {
			nextPageToken = table.NextPageToken
		}
	}
	return &listAllTablesResponse{
		Tables:        tables,
		NextPageToken: nextPageToken,
	}, nil
}

func (c deltaSharingRestClient) QueryTableMetadata(ctx context.Context, table Table) (*queryTableMetadataReponse, error) {
	pkg := "rest_client.go"
	fn := "QueryTableMetadata"
	url := "/shares/" + table.Share + "/schemas/" + table.Schema + "/tables/" + table.Name + "/metadata"
	rd, err := c.callSharingServer(ctx, url)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServer", err.Error()}
	}
	var metadata protoMetadata
	var p protocol
	if len(*rd) != 2 {
		return nil, &DSErr{pkg, fn, "len(*rd)", ""}
	}
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}

	err = json.Unmarshal((*rd)[1], &metadata)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	return &queryTableMetadataReponse{Metadata: metadata.Metadata, Protocol: p}, err
}

func (c deltaSharingRestClient) QueryTableVersion(ctx context.Context, table Table) (*queryTableVersionResponse, error) {
	pkg := "rest_client.go"
	fn := "QueryTableVersion"
	rawUrl := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name
	r, err := c.getResponseHeader(ctx, rawUrl)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.getResponseHeader", err.Error()}
	}
	i, err := strconv.ParseInt(r["Delta-Table-Version"][0], 10, 64)
	if err != nil {
		return nil, &DSErr{pkg, fn, "strconv.ParseInt", err.Error()}
	}
	return &queryTableVersionResponse{DeltaTableVersion: i}, err
}

func (c *deltaSharingRestClient) ListFilesInTable(ctx context.Context, table Table) (*listFilesInTableResponse, error) {
	pkg := "rest_client.go"
	fn := "ListFilesInTable"
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/query"
	rd, err := c.postQuery(ctx, url, []string{""}, 0)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.postQuery", err.Error()}
	}
	if len(*rd) < 3 {
		var error_msg delta_sharing_error
		err = json.Unmarshal((*rd)[0], &error_msg)
		if err == nil && error_msg.ErrorCode != "" {
			return nil, &DSErr{pkg, fn, "Delta Sharing Error", error_msg.ErrorMessage}
		}
		return nil, &DSErr{pkg, fn, "len(*rd)", "Array returned is too short"}
	}

	var p protocol
	var m protoMetadata
	var f protoFile
	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	err = json.Unmarshal((*rd)[1], &m)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	l := listFilesInTableResponse{Protocol: p, Metadata: m.Metadata}
	for _, v := range (*rd)[2:] {
		// Check context in loop
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		if len(v) == 0 {
			continue
		}
		err = json.Unmarshal(v, &f)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		l.AddFiles = append(l.AddFiles, f.File)
	}
	return &l, err
}

func (c *deltaSharingRestClient) postQuery(ctx context.Context, request string, predicateHints []string, limitHint int) (*[][]byte, error) {
	pkg := "rest_client.go"
	fn := "postQuery"
	// create request body
	rawURL := c.profile.Endpoint + "/" + request
	var responses [][]byte
	data := data{PredicateHints: predicateHints, LimitHint: limitHint}
	msg, err := json.Marshal(data)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Marshal", err.Error()}
	}
	reqBody := io.NopCloser(strings.NewReader(string(msg)))
	urlParsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, &DSErr{pkg, fn, "url.Parse", err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", urlParsed.String(), reqBody)
	if err != nil {
		return nil, &DSErr{pkg, fn, "http.NewRequestWithContext", err.Error()}
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("Authorization", "Bearer "+c.profile.BearerToken)

	var response *http.Response
	var retryCnt = 0

	for {
		// Check if context is cancelled before retry
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		response, err = http.DefaultClient.Do(req)
		if err != nil {
			if retryCnt <= c.numRetries && c.shouldRetry(response) {
				retryCnt++
				continue
			}

			return nil, &DSErr{pkg, fn, "http.DefaultClient.Do", err.Error()}
		} else {
			break
		}
	}

	defer response.Body.Close()
	var b bytes.Buffer
	_, err = io.Copy(&b, response.Body)
	if err != nil {
		return nil, &DSErr{pkg, fn, "io.Copy", err.Error()}
	}
	x := bytes.Split(b.Bytes(), []byte{'\n'})
	responses = append(responses, x...)
	return &responses, err
}

func (c *deltaSharingRestClient) shouldRetry(r *http.Response) bool {

	if r == nil {
		fmt.Println("Retry connection due to error")
		return true
	}
	if r.StatusCode == 429 {
		fmt.Println("Retry operation due to status code: 429")
		return true
	} else if r.StatusCode >= 500 && r.StatusCode < 600 {
		fmt.Printf("Retry operation due to status code: %d\n", r.StatusCode)
		return true
	} else {
		return false
	}
}

func (c *deltaSharingRestClient) ListTableChanges(ctx context.Context, table Table, options CdfOptions) (*listCdfFilesResponse, error) {
	pkg := "rest_client.go"
	fn := "ListTableChanges"
	url := "/shares/" + table.Share + "/schemas/" + strings.Trim(table.Schema, " ") + "/tables/" + table.Name + "/changes?"
	var params []string
	if options.StartingVersion != nil {
		params = append(params, "startingVersion="+fmt.Sprint(*options.StartingVersion))
	}
	if options.StartingTimestamp != nil {
		params = append(params, "startingTimestamp="+fmt.Sprint(*options.StartingTimestamp))
	}
	if options.EndingVersion != nil {
		params = append(params, "endingVersion="+fmt.Sprint(*options.EndingVersion))
	}
	if options.EndingTimestamp != nil {
		params = append(params, "endingTimestamp="+fmt.Sprint(*options.EndingTimestamp))
	}
	paramString := strings.Join(params, "&")
	rd, err := c.callSharingServer(ctx, url+paramString)
	if err != nil {
		return nil, &DSErr{pkg, fn, "c.callSharingServer", err.Error()}
	}
	if rd == nil || len(*rd) < 3 {
		var error_msg delta_sharing_error
		err = json.Unmarshal((*rd)[0], &error_msg)
		if err == nil && error_msg.ErrorCode != "" {
			return nil, &DSErr{pkg, fn, "Delta Sharing Error", error_msg.ErrorMessage}
		}
		return nil, &DSErr{pkg, fn, "len(*rd)", "Array returned is too short"}
	}
	var p protocol
	var m protoMetadata
	var f protoCdfFile

	err = json.Unmarshal((*rd)[0], &p)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	err = json.Unmarshal((*rd)[1], &m)
	if err != nil {
		return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
	}
	l := listCdfFilesResponse{Protocol: p, Metadata: m.Metadata}
	for _, v := range (*rd)[2:] {
		// Check context in loop
		select {
		case <-ctx.Done():
			return nil, &DSErr{pkg, fn, "context.Done", ctx.Err().Error()}
		default:
		}

		if len(v) == 0 {
			continue
		}
		err = json.Unmarshal(v, &f)
		if err != nil {
			return nil, &DSErr{pkg, fn, "json.Unmarshal", err.Error()}
		}
		if f.File != nil {
			l.Action.Add = append(l.Action.Add, *f.File)
		}
		if f.Cdf != nil {
			l.Action.Cdf = append(l.Action.Cdf, *f.Cdf)
		}
		if f.Remove != nil {
			l.Action.Remove = append(l.Action.Remove, *f.Remove)
		}
	}
	return &l, err
}

func (c *deltaSharingRestClient) ReadFileUrlToArrowTable(ctx context.Context, url string) (arrow.Table, error) {
	pf, err := c.readFileReader(ctx, url)
	if err != nil {
		return nil, err
	}
	mem := memory.NewGoAllocator()
	pa, err := pqarrow.ReadTable(ctx, pf, parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, err
	}

	return pa, nil
}
