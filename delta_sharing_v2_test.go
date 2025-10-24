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
	"testing"
	"time"
)

// ============================================================================
// EXPERIMENTAL V2 FUNCTION TESTS
// ============================================================================
//
// These tests cover the experimental V2 concurrent functions.
// Run with race detector: go test -race ./...
// ============================================================================

// TestListAllTables_V2_Success tests basic functionality
func TestListAllTables_V2_Success(t *testing.T) {
	// Create client with valid credentials
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()

	// Test with default concurrency (0 = auto)
	tables, token, err := client.ListAllTables_V2(ctx, 0, "", 0)
	if err != nil {
		t.Errorf("ListAllTables_V2() with default concurrency failed: %v", err)
		return
	}

	if len(tables) == 0 {
		t.Log("Warning: No tables returned (may be expected for test server)")
	}

	t.Logf("ListAllTables_V2 returned %d tables with token '%s'", len(tables), token)
}

// TestListAllTables_V2_CustomConcurrency tests with custom concurrency levels
func TestListAllTables_V2_CustomConcurrency(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()

	concurrencyLevels := []int{1, 5, 10, 20}

	for _, level := range concurrencyLevels {
		t.Run(t.Name()+"_Concurrency_"+string(rune(level+'0')), func(t *testing.T) {
			tables, _, err := client.ListAllTables_V2(ctx, 0, "", level)
			if err != nil {
				t.Errorf("ListAllTables_V2(concurrency=%d) failed: %v", level, err)
				return
			}
			t.Logf("Concurrency %d: returned %d tables", level, len(tables))
		})
	}
}

// TestListAllTables_V2_ContextCancellation tests context cancellation
func TestListAllTables_V2_ContextCancellation(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// This should likely fail due to timeout
	_, _, err = client.ListAllTables_V2(ctx, 0, "", 10)
	if err == nil {
		t.Log("Note: Expected context cancellation but operation succeeded (may be very fast)")
	} else {
		t.Logf("Context cancellation worked as expected: %v", err)
	}
}

// TestListAllTables_V2_EmptyShares tests behavior with no shares
func TestListAllTables_V2_EmptyShares(t *testing.T) {
	// This test would need a mock client that returns no shares
	// For now, we'll test that the function handles empty results gracefully
	t.Skip("Requires mock client implementation")
}

// TestLoadAllFilesInTable_V2_Success tests basic file loading
func TestLoadAllFilesInTable_V2_Success(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()

	// Use a known table from the test server
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// Test with default concurrency
	tables, err := client.LoadAllFilesInTable_V2(ctx, table, 0)
	if err != nil {
		t.Errorf("LoadAllFilesInTable_V2() failed: %v", err)
		return
	}

	// Clean up
	defer func() {
		for _, tbl := range tables {
			if tbl != nil {
				tbl.Release()
			}
		}
	}()

	if len(tables) == 0 {
		t.Error("Expected at least one table, got 0")
		return
	}

	t.Logf("LoadAllFilesInTable_V2 loaded %d files successfully", len(tables))

	// Verify tables are valid
	for i, tbl := range tables {
		if tbl == nil {
			t.Errorf("Table at index %d is nil", i)
			continue
		}
		if tbl.NumRows() < 0 {
			t.Errorf("Table at index %d has invalid row count: %d", i, tbl.NumRows())
		}
	}
}

// TestLoadAllFilesInTable_V2_CustomConcurrency tests different concurrency levels
func TestLoadAllFilesInTable_V2_CustomConcurrency(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	concurrencyLevels := []int{1, 5, 10}

	for _, level := range concurrencyLevels {
		t.Run(t.Name()+"_Concurrency_"+string(rune(level+'0')), func(t *testing.T) {
			tables, err := client.LoadAllFilesInTable_V2(ctx, table, level)
			if err != nil {
				t.Errorf("LoadAllFilesInTable_V2(concurrency=%d) failed: %v", level, err)
				return
			}

			defer func() {
				for _, tbl := range tables {
					if tbl != nil {
						tbl.Release()
					}
				}
			}()

			t.Logf("Concurrency %d: loaded %d files", level, len(tables))
		})
	}
}

// TestLoadAllFilesInTable_V2_ContextCancellation tests context cancellation
func TestLoadAllFilesInTable_V2_ContextCancellation(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	tables, err := client.LoadAllFilesInTable_V2(ctx, table, 10)
	if err == nil {
		// Clean up if somehow succeeded
		for _, tbl := range tables {
			if tbl != nil {
				tbl.Release()
			}
		}
		t.Log("Note: Expected context cancellation but operation succeeded (may be very fast)")
	} else {
		t.Logf("Context cancellation worked as expected: %v", err)
	}
}

// TestLoadAllFilesInTable_V2_InvalidTable tests error handling
func TestLoadAllFilesInTable_V2_InvalidTable(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()

	// Use a non-existent table
	table := Table{
		Name:   "nonexistent-table",
		Share:  "delta_sharing",
		Schema: "default",
	}

	tables, err := client.LoadAllFilesInTable_V2(ctx, table, 10)
	if err == nil {
		// Clean up if somehow succeeded
		for _, tbl := range tables {
			if tbl != nil {
				tbl.Release()
			}
		}
		t.Error("Expected error for invalid table, got nil")
	} else {
		t.Logf("Error handling worked correctly: %v", err)
	}
}

// TestLoadFilesConcurrently_V2_Success tests basic functionality
func TestLoadFilesConcurrently_V2_Success(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// First, get the file list to know which IDs to request
	fileList, err := client.ListFilesInTable(ctx, table)
	if err != nil {
		t.Errorf("ListFilesInTable() failed: %v", err)
		return
	}

	if len(fileList.AddFiles) == 0 {
		t.Skip("No files in table to test with")
	}

	// Get first file ID (or multiple if available)
	var fileIDs []string
	numFilesToTest := min(len(fileList.AddFiles), 3)
	for i := 0; i < numFilesToTest; i++ {
		fileIDs = append(fileIDs, fileList.AddFiles[i].Id)
	}

	// Load files concurrently
	tableMap, err := client.LoadFilesConcurrently_V2(ctx, table, fileIDs, 0)
	if err != nil {
		t.Errorf("LoadFilesConcurrently_V2() failed: %v", err)
		return
	}

	// Clean up
	defer func() {
		for _, tbl := range tableMap {
			if tbl != nil {
				tbl.Release()
			}
		}
	}()

	if len(tableMap) != len(fileIDs) {
		t.Errorf("Expected %d tables, got %d", len(fileIDs), len(tableMap))
	}

	// Verify all requested file IDs are present
	for _, id := range fileIDs {
		if _, ok := tableMap[id]; !ok {
			t.Errorf("File ID %s not found in result map", id)
		}
	}

	t.Logf("LoadFilesConcurrently_V2 loaded %d files successfully", len(tableMap))
}

// TestLoadFilesConcurrently_V2_EmptyFileList tests with empty file list
func TestLoadFilesConcurrently_V2_EmptyFileList(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// Request empty file list
	tableMap, err := client.LoadFilesConcurrently_V2(ctx, table, []string{}, 10)
	if err != nil {
		t.Errorf("LoadFilesConcurrently_V2() with empty list failed: %v", err)
		return
	}

	if len(tableMap) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(tableMap))
	}

	t.Log("Empty file list handled correctly")
}

// TestLoadFilesConcurrently_V2_InvalidFileID tests error handling
func TestLoadFilesConcurrently_V2_InvalidFileID(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	ctx := context.Background()
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// Request non-existent file IDs
	fileIDs := []string{"invalid-file-id-1", "invalid-file-id-2"}

	tableMap, err := client.LoadFilesConcurrently_V2(ctx, table, fileIDs, 10)
	if err == nil {
		// Clean up if somehow succeeded
		for _, tbl := range tableMap {
			if tbl != nil {
				tbl.Release()
			}
		}
		t.Error("Expected error for invalid file IDs, got nil")
	} else {
		t.Logf("Error handling worked correctly: %v", err)
	}
}

// TestLoadFilesConcurrently_V2_ContextCancellation tests context cancellation
func TestLoadFilesConcurrently_V2_ContextCancellation(t *testing.T) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		t.Skipf("Skipping test: cannot create client: %v", err)
	}

	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	// First get valid file IDs
	fileList, err := client.ListFilesInTable(context.Background(), table)
	if err != nil || len(fileList.AddFiles) == 0 {
		t.Skip("Cannot get file list for test")
	}

	fileIDs := []string{fileList.AddFiles[0].Id}

	// Create context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	tableMap, err := client.LoadFilesConcurrently_V2(ctx, table, fileIDs, 10)
	if err == nil {
		// Clean up if somehow succeeded
		for _, tbl := range tableMap {
			if tbl != nil {
				tbl.Release()
			}
		}
		t.Log("Note: Expected context cancellation but operation succeeded (may be very fast)")
	} else {
		t.Logf("Context cancellation worked as expected: %v", err)
	}
}

// BenchmarkListAllTables_V1_vs_V2 compares performance
func BenchmarkListAllTables_V1_vs_V2(b *testing.B) {
	clientV2, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		b.Skipf("Skipping benchmark: cannot create client: %v", err)
	}

	ctx := context.Background()

	b.Run("V1_Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := clientV2.ListAllTables(ctx, 0, "")
			if err != nil {
				b.Errorf("V1 failed: %v", err)
			}
		}
	})

	b.Run("V2_Parallel_Default", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := clientV2.ListAllTables_V2(ctx, 0, "", 0)
			if err != nil {
				b.Errorf("V2 failed: %v", err)
			}
		}
	})

	b.Run("V2_Parallel_Concurrency5", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := clientV2.ListAllTables_V2(ctx, 0, "", 5)
			if err != nil {
				b.Errorf("V2 failed: %v", err)
			}
		}
	})

	b.Run("V2_Parallel_Concurrency10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := clientV2.ListAllTables_V2(ctx, 0, "", 10)
			if err != nil {
				b.Errorf("V2 failed: %v", err)
			}
		}
	})
}

// BenchmarkLoadAllFilesInTable_V2 benchmarks file loading with different concurrency
func BenchmarkLoadAllFilesInTable_V2(b *testing.B) {
	client, err := NewSharingClientV2("test_profile.json")
	if err != nil {
		b.Skipf("Skipping benchmark: cannot create client: %v", err)
	}

	ctx := context.Background()
	table := Table{
		Name:   "boston-housing",
		Share:  "delta_sharing",
		Schema: "default",
	}

	concurrencyLevels := []int{1, 5, 10, 20}

	for _, level := range concurrencyLevels {
		b.Run("Concurrency_"+string(rune(level+'0')), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tables, err := client.LoadAllFilesInTable_V2(ctx, table, level)
				if err != nil {
					b.Errorf("LoadAllFilesInTable_V2 failed: %v", err)
					continue
				}
				// Clean up
				for _, tbl := range tables {
					if tbl != nil {
						tbl.Release()
					}
				}
			}
		})
	}
}
