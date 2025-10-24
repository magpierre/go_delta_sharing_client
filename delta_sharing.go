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
	"sync"

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
	GetTableMetadata(ctx context.Context, t Table) (*Metadata, error)
	ListTableChanges(ctx context.Context, t Table, options CdfOptions) (*listCdfFilesResponse, error)
	ReadFileUrlToArrowTable(ctx context.Context, url string) (arrow.Table, error)
}

// SharingClientV2 extends SharingClient with EXPERIMENTAL concurrent operations.
//
// ⚠️  EXPERIMENTAL: This interface includes methods that use goroutines for
// improved performance. These methods are marked with _V2 suffix and their
// API may change in future versions.
//
// This interface embeds SharingClient, so all stable methods are available
// alongside the experimental V2 methods.
//
// Usage:
//
//	// Opt-in to experimental features
//	clientV2, err := ds.NewSharingClientV2("profile.json")
//	if err != nil {
//	    return err
//	}
//
//	// Use stable methods (inherited from SharingClient)
//	tables, _, err := clientV2.ListAllTables(ctx, 0, "")
//
//	// Use experimental V2 methods
//	tables, _, err := clientV2.ListAllTables_V2(ctx, 0, "", 10)
//
// For production use, consider using SharingClient instead until V2 methods
// are proven stable.
type SharingClientV2 interface {
	SharingClient // Embeds all stable methods

	// ⚠️  EXPERIMENTAL V2 METHODS BELOW

	// ListAllTables_V2 fetches tables from all shares concurrently.
	// See method documentation for details.
	ListAllTables_V2(ctx context.Context, maxResults int, pageToken string, maxConcurrency int) ([]Table, string, error)

	// LoadAllFilesInTable_V2 loads all files from a table concurrently.
	// See method documentation for details.
	LoadAllFilesInTable_V2(ctx context.Context, table Table, maxConcurrency int) ([]arrow.Table, error)

	// LoadFilesConcurrently_V2 loads specific files by ID concurrently.
	// See method documentation for details.
	LoadFilesConcurrently_V2(ctx context.Context, table Table, fileIDs []string, maxConcurrency int) (map[string]arrow.Table, error)
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

// NewSharingClientV2 creates a Delta Sharing client with EXPERIMENTAL V2 features.
//
// ⚠️  EXPERIMENTAL: This returns a SharingClientV2 interface which includes
// experimental concurrent operations (_V2 methods). Use with caution.
//
// The returned client implements both SharingClient (stable) and SharingClientV2
// (experimental) interfaces, so you can use both stable and V2 methods.
//
// Parameters:
//   - ProfileFile: Path to Delta Sharing profile JSON file
//
// Returns:
//   - SharingClientV2 interface with all stable and experimental methods
//   - Error if profile cannot be loaded
//
// Example:
//
//	clientV2, err := ds.NewSharingClientV2("profile.json")
//	if err != nil {
//	    return err
//	}
//
//	// Use stable methods
//	tables, _, _ := clientV2.ListAllTables(ctx, 0, "")
//
//	// Use experimental V2 methods
//	tables, _, _ := clientV2.ListAllTables_V2(ctx, 0, "", 10)
func NewSharingClientV2(ProfileFile string) (SharingClientV2, error) {
	client, err := NewSharingClient(ProfileFile)
	if err != nil {
		return nil, err
	}
	// sharingClient implements both SharingClient and SharingClientV2
	return client.(*sharingClient), nil
}

// NewSharingClientV2FromString creates a Delta Sharing client with EXPERIMENTAL
// V2 features from a JSON string.
//
// ⚠️  EXPERIMENTAL: Returns SharingClientV2 with experimental concurrent operations.
//
// Parameters:
//   - ProfileString: Delta Sharing profile as JSON string
//
// Returns:
//   - SharingClientV2 interface with all stable and experimental methods
//   - Error if profile cannot be parsed
//
// Example:
//
//	profile := `{"shareCredentialsVersion":1,"endpoint":"...","bearerToken":"..."}`
//	clientV2, err := ds.NewSharingClientV2FromString(profile)
func NewSharingClientV2FromString(ProfileString string) (SharingClientV2, error) {
	client, err := NewSharingClientFromString(ProfileString)
	if err != nil {
		return nil, err
	}
	// sharingClient implements both SharingClient and SharingClientV2
	return client.(*sharingClient), nil
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

func (s *sharingClient) GetTableMetadata(ctx context.Context, t Table) (*Metadata, error) {
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

// ============================================================================
// EXPERIMENTAL V2 FUNCTIONS - CONCURRENT/PARALLEL OPERATIONS
// ============================================================================
//
// The following functions are EXPERIMENTAL and may change in future versions.
// They use goroutines for improved performance but are still being tested.
// Use at your own risk in production environments.
//
// All V2 functions follow these principles:
// - Bounded concurrency to prevent resource exhaustion
// - Context-aware for cancellation support
// - Fail-fast error handling (first error cancels all operations)
// - Same API contracts as non-V2 versions where applicable
//
// For more details, see: plans/goroutine-performance-improvements.md
// ============================================================================

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ListAllTables_V2 is an EXPERIMENTAL parallel version of ListAllTables.
//
// ⚠️  EXPERIMENTAL: This function uses goroutines for concurrent execution.
// API and behavior may change in future versions.
//
// Fetches tables from all shares concurrently using a worker pool pattern.
// This can be significantly faster (up to 10x) when there are multiple shares,
// but uses more resources (goroutines, connections).
//
// Parameters:
//   - ctx: Context for cancellation
//   - maxResults: Maximum results per share (0 for unlimited)
//   - pageToken: Pagination token
//   - maxConcurrency: Maximum number of concurrent share queries (0 = auto, default 10)
//
// Returns:
//   - Tables from all shares (order not guaranteed)
//   - Next page token (if applicable)
//   - Error if any operation fails (fail-fast behavior)
//
// Example:
//
//	// Use default concurrency (10)
//	tables, token, err := client.ListAllTables_V2(ctx, 0, "", 0)
//
//	// Use custom concurrency
//	tables, token, err := client.ListAllTables_V2(ctx, 0, "", 5)
func (s *sharingClient) ListAllTables_V2(
	ctx context.Context,
	maxResults int,
	pageToken string,
	maxConcurrency int,
) ([]Table, string, error) {
	pkg := "delta_sharing.go"
	fn := "ListAllTables_V2"

	// Get all shares
	sh, err := s.restClient.ListShares(ctx, 0, "")
	if err != nil {
		return nil, "", &DSErr{pkg, fn, "ListShares", err.Error()}
	}

	if len(sh.Shares) == 0 {
		return []Table{}, "", nil
	}

	// Determine concurrency level
	if maxConcurrency <= 0 {
		maxConcurrency = 10 // Default
	}
	workerCount := min(len(sh.Shares), maxConcurrency)

	// Result collection
	type result struct {
		tables []Table
		token  string
		err    error
		index  int // For debugging/ordering if needed
	}

	results := make(chan result, len(sh.Shares))
	semaphore := make(chan struct{}, workerCount)

	// Context for cancellation propagation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Launch goroutines for each share
	var wg sync.WaitGroup
	for i, shr := range sh.Shares {
		wg.Add(1)
		go func(index int, shareObj share) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results <- result{err: ctx.Err(), index: index}
				return
			}

			// Fetch tables for this share
			tablesResp, err := s.restClient.ListAllTables(ctx, shareObj, maxResults, pageToken)
			if err != nil {
				results <- result{err: err, index: index}
				return
			}

			results <- result{
				tables: tablesResp.Tables,
				token:  tablesResp.NextPageToken,
				err:    nil,
				index:  index,
			}
		}(i, shr)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allTables []Table
	var lastToken string
	for res := range results {
		if res.err != nil {
			cancel() // Cancel remaining operations
			return nil, "", &DSErr{pkg, fn, fmt.Sprintf("share %d", res.index), res.err.Error()}
		}
		allTables = append(allTables, res.tables...)
		if res.token != "" {
			lastToken = res.token
		}
	}

	return allTables, lastToken, nil
}

// LoadAllFilesInTable_V2 is an EXPERIMENTAL function that loads all files
// from a Delta table concurrently and returns them as Arrow tables.
//
// ⚠️  EXPERIMENTAL: This function uses goroutines for concurrent file loading.
// API and behavior may change in future versions.
//
// This function:
// 1. Lists all files in the table
// 2. Downloads and converts each file to Arrow table in parallel
// 3. Returns slice of Arrow tables in the same order as AddFiles
//
// WARNING: This can use significant memory and network resources for tables
// with many files. Consider using smaller maxConcurrency values for large tables.
//
// Parameters:
//   - ctx: Context for cancellation
//   - client: Delta Sharing client
//   - table: Table to load
//   - maxConcurrency: Maximum number of concurrent downloads (0 = auto, default 20)
//
// Returns:
//   - Slice of Arrow tables (one per file, in AddFiles order)
//   - Error if any operation fails
//
// Memory Management:
//   - Caller must release all returned tables
//   - Use defer with a loop to ensure cleanup
//
// Example:
//
//	tables, err := ds.LoadAllFilesInTable_V2(ctx, client, table, 10)
//	if err != nil {
//	    return err
//	}
//	defer func() {
//	    for _, t := range tables {
//	        if t != nil {
//	            t.Release()
//	        }
//	    }
//	}()
func (s *sharingClient) LoadAllFilesInTable_V2(
	ctx context.Context,
	table Table,
	maxConcurrency int,
) ([]arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadAllFilesInTable_V2"

	// List files in table
	fileList, err := s.ListFilesInTable(ctx, table)
	if err != nil {
		return nil, &DSErr{pkg, fn, "ListFilesInTable", err.Error()}
	}

	if len(fileList.AddFiles) == 0 {
		return []arrow.Table{}, nil
	}

	// Determine concurrency level
	if maxConcurrency <= 0 {
		maxConcurrency = 20 // Default
	}
	workerCount := min(len(fileList.AddFiles), maxConcurrency)

	// Pre-allocate results slice to maintain order
	results := make([]arrow.Table, len(fileList.AddFiles))
	errChan := make(chan error, len(fileList.AddFiles))
	semaphore := make(chan struct{}, workerCount)

	// Context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// Launch goroutines for each file
	for i, file := range fileList.AddFiles {
		wg.Add(1)
		go func(index int, fileURL string) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}

			// Load file as Arrow table
			arrowTable, err := s.ReadFileUrlToArrowTable(ctx, fileURL)
			if err != nil {
				errChan <- &DSErr{pkg, fn, fmt.Sprintf("file %d", index), err.Error()}
				return
			}

			results[index] = arrowTable
			errChan <- nil
		}(i, file.Url)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors (fail-fast)
	for err := range errChan {
		if err != nil {
			cancel() // Cancel remaining operations

			// Clean up any loaded tables
			for _, t := range results {
				if t != nil {
					t.Release()
				}
			}

			return nil, err
		}
	}

	return results, nil
}

// LoadFilesConcurrently_V2 is an EXPERIMENTAL function that loads specific
// files from a Delta table by their IDs concurrently.
//
// ⚠️  EXPERIMENTAL: This function uses goroutines for concurrent file loading.
// API and behavior may change in future versions.
//
// This function:
// 1. Lists all files in the table
// 2. Filters to only the requested file IDs
// 3. Downloads and converts each file to Arrow table in parallel
// 4. Returns map of fileID -> Arrow table
//
// Parameters:
//   - ctx: Context for cancellation
//   - client: Delta Sharing client
//   - table: Table to load from
//   - fileIDs: Slice of file IDs to load
//   - maxConcurrency: Maximum number of concurrent downloads (0 = auto, default 20)
//
// Returns:
//   - Map of file ID to Arrow table
//   - Error if any operation fails or if any fileID is not found
//
// Memory Management:
//   - Caller must release all returned tables
//   - Iterate over map values and call Release()
//
// Example:
//
//	fileIDs := []string{"file-abc", "file-def", "file-xyz"}
//	tables, err := ds.LoadFilesConcurrently_V2(ctx, client, table, fileIDs, 5)
//	if err != nil {
//	    return err
//	}
//	defer func() {
//	    for _, t := range tables {
//	        if t != nil {
//	            t.Release()
//	        }
//	    }
//	}()
func (s *sharingClient) LoadFilesConcurrently_V2(
	ctx context.Context,
	table Table,
	fileIDs []string,
	maxConcurrency int,
) (map[string]arrow.Table, error) {
	pkg := "delta_sharing.go"
	fn := "LoadFilesConcurrently_V2"

	if len(fileIDs) == 0 {
		return make(map[string]arrow.Table), nil
	}

	// List files in table
	fileList, err := s.ListFilesInTable(ctx, table)
	if err != nil {
		return nil, &DSErr{pkg, fn, "ListFilesInTable", err.Error()}
	}

	// Create lookup map for requested file IDs
	requestedFiles := make(map[string]bool)
	for _, id := range fileIDs {
		requestedFiles[id] = true
	}

	// Filter files to only those requested
	type fileInfo struct {
		id  string
		url string
	}
	var filesToLoad []fileInfo
	for _, file := range fileList.AddFiles {
		if requestedFiles[file.Id] {
			filesToLoad = append(filesToLoad, fileInfo{id: file.Id, url: file.Url})
		}
	}

	// Check if all requested files were found
	if len(filesToLoad) != len(fileIDs) {
		return nil, &DSErr{pkg, fn, "validate fileIDs", fmt.Sprintf(
			"found %d of %d requested files", len(filesToLoad), len(fileIDs))}
	}

	// Determine concurrency level
	if maxConcurrency <= 0 {
		maxConcurrency = 20 // Default
	}
	workerCount := min(len(filesToLoad), maxConcurrency)

	// Results collection
	type result struct {
		id    string
		table arrow.Table
		err   error
	}
	results := make(chan result, len(filesToLoad))
	semaphore := make(chan struct{}, workerCount)

	// Context for cancellation
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// Launch goroutines for each file
	for _, fInfo := range filesToLoad {
		wg.Add(1)
		go func(fi fileInfo) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results <- result{id: fi.id, err: ctx.Err()}
				return
			}

			// Load file as Arrow table
			arrowTable, err := s.ReadFileUrlToArrowTable(ctx, fi.url)
			if err != nil {
				results <- result{id: fi.id, err: &DSErr{pkg, fn, "ReadFileUrlToArrowTable", err.Error()}}
				return
			}

			results <- result{id: fi.id, table: arrowTable, err: nil}
		}(fInfo)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	tableMap := make(map[string]arrow.Table)
	for res := range results {
		if res.err != nil {
			cancel() // Cancel remaining operations

			// Clean up any loaded tables
			for _, t := range tableMap {
				if t != nil {
					t.Release()
				}
			}

			return nil, res.err
		}
		tableMap[res.id] = res.table
	}

	return tableMap, nil
}
