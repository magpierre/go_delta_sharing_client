# Go Delta Sharing Client

A community-developed Delta Sharing client library for Golang, providing type-safe access to Delta Shares for applications written in Go.

## Overview

This library implements the [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing) to enable secure data sharing across organizations. It provides a high-level Go API for querying shared datasets, reading Parquet files, and tracking incremental changes via Change Data Feed (CDF).

## Features

- **Full Delta Sharing Protocol Support**: List shares, schemas, tables, and query table metadata
- **Apache Arrow Integration**: Efficient columnar data representation using Arrow v18.4.1
- **Change Data Feed (CDF)**: Query incremental table changes by version or timestamp
- **Pagination Support**: Handle large result sets with built-in pagination
- **Context Support**: Cancellation and timeout control for all operations
- **Type Safety**: Proper 64-bit integer types matching Delta Sharing protocol specification
- **Retry Logic**: Configurable automatic retries for transient failures (429, 5xx errors)
- **Flexible Authentication**: Load profiles from files or JSON strings

## Installation

```bash
go get github.com/magnuspierre/go_delta_sharing_client
```

**Requirements**: Go 1.23 or higher

## Quick Start

### 1. Create a Profile Configuration

Create a Delta Sharing profile JSON file (e.g., `profile.json`):

```json
{
  "shareCredentialsVersion": 1,
  "bearerToken": "your-bearer-token",
  "endpoint": "https://sharing.delta.io/delta-sharing/"
}
```

### 2. Initialize the Client

```go
package main

import (
    "context"
    "fmt"
    "log"

    ds "github.com/magnuspierre/go_delta_sharing_client"
)

func main() {
    // Create client from profile file
    client, err := ds.NewSharingClient("profile.json")
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // List all available shares
    shares, _, err := client.ListShares(ctx, 0, "")
    if err != nil {
        log.Fatal(err)
    }

    for _, share := range shares {
        fmt.Printf("Share: %s\n", share.Name)
    }
}
```

### 3. Load Data into Apache Arrow

```go
// Define your table
table := ds.Table{
    Share:  "delta_sharing",
    Schema: "default",
    Name:   "covid_19_nyt",
}

// List files in the table
response, err := client.ListFilesInTable(ctx, table)
if err != nil {
    log.Fatal(err)
}

// Load first file as Arrow table
if len(response.AddFiles) > 0 {
    arrowTable, err := client.ReadFileAsArrowTable(ctx, response.AddFiles[0])
    if err != nil {
        log.Fatal(err)
    }
    defer arrowTable.Release()

    fmt.Printf("Loaded %d rows, %d columns\n",
        arrowTable.NumRows(), arrowTable.NumCols())
}
```

## API Reference

### Client Creation

```go
// From file path
client, err := NewSharingClient("profile.json")

// From JSON string
profileJSON := `{"shareCredentialsVersion": 1, "bearerToken": "...", "endpoint": "..."}`
client, err := NewSharingClientFromString(profileJSON)
```

### Listing Operations (with Pagination)

All list operations support pagination via `maxResults` and `pageToken` parameters:

```go
// List shares (maxResults=0 means no limit)
shares, nextToken, err := client.ListShares(ctx, 0, "")

// List schemas in a share
schemas, nextToken, err := client.ListSchemas(ctx, share, 0, "")

// List tables in a schema
tables, nextToken, err := client.ListTables(ctx, schema, 0, "")

// List all tables across all shares
allTables, nextToken, err := client.ListAllTables(ctx, 0, "")

// Paginated example: get 100 items per page
pageToken := ""
for {
    tables, nextToken, err := client.ListTables(ctx, schema, 100, pageToken)
    if err != nil {
        return err
    }

    // Process tables...

    if nextToken == "" {
        break // No more pages
    }
    pageToken = nextToken
}
```

### Table Operations

```go
// Get table metadata
metadata, err := client.GetTableMetadata(ctx, table)

// Get table version
version, err := client.GetTableVersion(ctx, table)

// List files in table
response, err := client.ListFilesInTable(ctx, table)

// List files with version filter
response, err := client.ListFilesInTableWithVersion(ctx, table, version, timestamp)

// Read file as Arrow table
arrowTable, err := client.ReadFileAsArrowTable(ctx, file)
```

### Change Data Feed (CDF)

Query incremental changes to track table updates:

```go
// Query by version range
cdfOptions := ds.CdfOptions{
    StartingVersion: 0,
    EndingVersion:   10,
}
changes, err := client.ListTableChanges(ctx, table, cdfOptions)

// Query by timestamp range
cdfOptions := ds.CdfOptions{
    StartingTimestamp: "2023-01-01T00:00:00Z",
    EndingTimestamp:   "2023-12-31T23:59:59Z",
}
changes, err := client.ListTableChanges(ctx, table, cdfOptions)

// Process changes
for _, addFile := range changes.AddFiles {
    fmt.Printf("Added: %s\n", addFile.Id)
}
for _, cdfFile := range changes.Cdf {
    fmt.Printf("Changed: %s\n", cdfFile.Id)
}
for _, removeFile := range changes.Remove {
    fmt.Printf("Removed: %s\n", removeFile.Id)
}
```

### Helper Functions

```go
// Parse Delta Sharing URL (format: "profile#share.schema.table")
url := "profile.json#delta_sharing.default.covid_19_nyt"
client, table, err := parseURL(url)

// Load Arrow table directly from URL and file index
arrowTable, err := LoadAsArrowTable(ctx, url, 0)

// Load specific file by ID
arrowTable, err := LoadArrowTable(ctx, client, table, "file-id")
```

## Context Support

All operations accept `context.Context` for cancellation and timeout control:

```go
// With timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

shares, _, err := client.ListShares(ctx, 0, "")

// With cancellation
ctx, cancel := context.WithCancel(context.Background())
go func() {
    time.Sleep(5 * time.Second)
    cancel() // Cancel after 5 seconds
}()

tables, _, err := client.ListTables(ctx, schema, 0, "")
```

## Architecture

The library is organized into three layers:

### 1. Protocol Layer (`protocol.go`)
- Defines Delta Sharing protocol data structures
- Types: `deltaSharingProfile`, `share`, `schema`, `Table`, `File`, `metadata`, `protocol`
- Custom error type: `DSErr` with package/function/call context
- JSON serialization/deserialization

### 2. REST Client Layer (`rest_client.go`)
- Low-level HTTP client: `deltaSharingRestClient`
- Implements all API endpoints
- Retry logic for transient failures (configurable, default 5 retries)
- Bearer token authentication
- Parses JSON-newline delimited responses

### 3. High-Level Client Layer (`delta_sharing.go`)
- Public API: `SharingClient` interface
- Concrete implementation: `sharingClient`
- Helper functions for common operations
- Arrow integration for data loading

## Building and Testing

### Build

```bash
# Build the package
go build

# Verify dependencies
go mod verify

# Tidy dependencies
go mod tidy
```

### Test

```bash
# Run all tests
go test ./...

# Run with verbose output
go test -v ./...

# Run specific test
go test -v -run TestFunctionName

# Run with benchmarks
go test -bench=. -benchmem ./...
```

**Test Coverage**: 51 tests covering protocol, REST client, high-level API, context handling, and pagination.

## Type Specifications

All numeric fields use correct 64-bit integer types per Delta Sharing protocol:

- **File struct**: `Size`, `Timestamp`, `Version`, `ExpirationTimestamp` are `int64`
- **metadata struct**: `Version`, `Size`, `NumFiles` are `int64`
- **API returns**: `GetTableVersion()` returns `int64`

## Dependencies

- **Apache Arrow v18.4.1** (`github.com/apache/arrow-go/v18`): Columnar data representation
  - SeekToRow for pagination
  - Bloom filters for query optimization
  - Performance improvements and security patches

## Error Handling

The library uses a custom error type `DSErr` that provides detailed context:

```go
type DSErr struct {
    Module   string
    Function string
    Call     string
    Err      error
}
```

Errors include the originating module, function name, and specific call site for easier debugging.

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass: `go test ./...`
2. Code follows Go conventions: `go fmt ./...`
3. New features include tests
4. Update documentation as needed

## License

   Copyright 2025 Magnus Pierre

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

## Resources

- [Delta Sharing Protocol Specification](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md)
- [Apache Arrow Go Documentation](https://pkg.go.dev/github.com/apache/arrow-go/v18)
- [Delta Sharing Website](https://delta.io/sharing/)

## Acknowledgments

This is a community-developed library for the Delta Sharing ecosystem. Special thanks to the Delta Lake community.
