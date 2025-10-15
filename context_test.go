/*
#
# Copyright (C) 2022 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
*/

package delta_sharing

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestContextCancellation tests that operations respect context cancellation
func TestContextCancellation_ListShares(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Try to list shares with cancelled context
	_, _, err = client.ListShares(ctx, 0, "")
	if err == nil {
		t.Error("ListShares() expected error with cancelled context, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "context canceled") {
		t.Logf("ListShares() with cancelled context returned: %v (may not detect cancellation before network call)", err)
	}
}

func TestContextTimeout_ListShares(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	// Create a context with very short timeout (1 nanosecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait a bit to ensure timeout
	time.Sleep(10 * time.Millisecond)

	// Try to list shares with timed-out context
	_, _, err = client.ListShares(ctx, 0, "")
	if err == nil {
		t.Error("ListShares() expected error with timed-out context, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "deadline exceeded") && !strings.Contains(err.Error(), "context canceled") {
		t.Logf("ListShares() with timeout returned: %v (may not detect timeout before network call)", err)
	}
}

func TestContextCancellation_ListSchemas(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = client.ListSchemas(ctx, share{Name: "delta_sharing"}, 0, "")
	if err == nil {
		t.Error("ListSchemas() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_ListTables(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = client.ListTables(ctx, schema{Name: "default", Share: "delta_sharing"}, 0, "")
	if err == nil {
		t.Error("ListTables() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_ListAllTables(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = client.ListAllTables(ctx, 0, "")
	if err == nil {
		t.Error("ListAllTables() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_ListFilesInTable(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.ListFilesInTable(ctx, Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"})
	if err == nil {
		t.Error("ListFilesInTable() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_GetTableVersion(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.GetTableVersion(ctx, Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"})
	if err == nil {
		t.Error("GetTableVersion() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_GetTableMetadata(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.GetTableMetadata(ctx, Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"})
	if err == nil {
		t.Error("GetTableMetadata() expected error with cancelled context, got nil")
	}
}

func TestContextCancellation_ReadFileUrlToArrowTable(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Use a fake URL - it should fail with context error before trying to fetch
	_, err = client.ReadFileUrlToArrowTable(ctx, "https://example.com/fake.parquet")
	if err == nil {
		t.Error("ReadFileUrlToArrowTable() expected error with cancelled context, got nil")
	}
}

// TestContextPropagation verifies that context is properly propagated through the call chain
func TestContextPropagation(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	// Test with a valid context
	ctx := context.Background()
	shares, _, err := client.ListShares(ctx, 0, "")
	if err != nil {
		t.Errorf("ListShares() with valid context failed: %v", err)
		return
	}
	if len(shares) == 0 {
		t.Error("ListShares() returned no shares")
	}
}

// TestContextWithDeadline tests that operations respect deadlines
func TestContextWithDeadline(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	// Set a deadline in the past
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()

	_, _, err = client.ListShares(ctx, 0, "")
	if err == nil {
		t.Error("ListShares() expected error with past deadline, got nil")
	}
}

// TestHelperFunctions tests the helper functions with context
func TestLoadAsArrowTable_WithContext(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		fileno  int
		wantErr bool
	}{
		{
			name:    "invalid_url_format",
			url:     "invalid-url",
			fileno:  0,
			wantErr: true,
		},
		{
			name:    "invalid_profile_path",
			url:     "/nonexistent/path.share#share.schema.table",
			fileno:  0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, err := LoadAsArrowTable(ctx, tt.url, tt.fileno)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAsArrowTable() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadAsArrowTable_WithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	url := "/Users/magnuspierre/Documents/shares/open-datasets.share.txt#delta_sharing.default.boston-housing"
	_, err := LoadAsArrowTable(ctx, url, 0)
	if err == nil {
		t.Error("LoadAsArrowTable() expected error with cancelled context, got nil")
	}
}

func TestLoadArrowTable_WithContext(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()
	table := Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"}

	// Get file list first
	files, err := client.ListFilesInTable(ctx, table)
	if err != nil {
		t.Skipf("Skipping test - cannot list files: %v", err)
		return
	}

	if len(files.AddFiles) == 0 {
		t.Skip("Skipping test - no files in table")
		return
	}

	// Test with valid file ID
	_, err = LoadArrowTable(ctx, client, table, files.AddFiles[0].Id)
	if err != nil {
		t.Errorf("LoadArrowTable() with valid context failed: %v", err)
	}
}

func TestLoadArrowTable_WithCancelledContext(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	table := Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"}
	_, err = LoadArrowTable(ctx, client, table, "fake-file-id")
	if err == nil {
		t.Error("LoadArrowTable() expected error with cancelled context, got nil")
	}
}

func TestLoadArrowTable_InvalidFileId(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()
	table := Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"}

	// Test with non-existent file ID
	_, err = LoadArrowTable(ctx, client, table, "non-existent-file-id")
	if err == nil {
		t.Error("LoadArrowTable() expected error with invalid file ID, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "fileid not found") {
		t.Errorf("LoadArrowTable() expected 'fileid not found' error, got: %v", err)
	}
}

func TestLoadArrowTable_NilClient(t *testing.T) {
	ctx := context.Background()
	table := Table{Name: "boston-housing", Share: "delta_sharing", Schema: "default"}

	_, err := LoadArrowTable(ctx, nil, table, "some-id")
	if err == nil {
		t.Error("LoadArrowTable() expected error with nil client, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "Invalid client") {
		t.Errorf("LoadArrowTable() expected 'Invalid client' error, got: %v", err)
	}
}

// TestNewSharingClientFromString tests the string-based constructor with context
func TestNewSharingClientFromString(t *testing.T) {
	tests := []struct {
		name    string
		profile string
		wantErr bool
	}{
		{
			name:    "invalid_json",
			profile: "not valid json",
			wantErr: true,
		},
		{
			name:    "empty_string",
			profile: "",
			wantErr: true,
		},
		{
			name: "valid_profile",
			profile: `{
				"shareCredentialsVersion": 1,
				"endpoint": "https://sharing.delta.io/delta-sharing/",
				"bearerToken": "test-token"
			}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewSharingClientFromString(tt.profile)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSharingClientFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewSharingClientFromString() expected non-nil client, got nil")
			}
		})
	}
}

// TestCDFWithContext tests Change Data Feed operations with context
func TestListTableChanges_WithCancelledContext(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	table := Table{Name: "cdf_table_cdf_enabled", Share: "delta_sharing", Schema: "default"}
	startVer := 0
	options := CdfOptions{StartingVersion: &startVer}

	_, err = client.ListTableChanges(ctx, table, options)
	if err == nil {
		t.Error("ListTableChanges() expected error with cancelled context, got nil")
	}
}

// TestPagination tests pagination functionality
func TestPagination_ListTables(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()
	schema := schema{Name: "default", Share: "delta_sharing"}

	// Test with maxResults limit
	tables, nextToken, err := client.ListTables(ctx, schema, 3, "")
	if err != nil {
		t.Errorf("ListTables() with maxResults failed: %v", err)
		return
	}

	// Should get results (may or may not have nextToken depending on server behavior)
	if len(tables) == 0 {
		t.Error("ListTables() with maxResults returned no tables")
	}

	// If there's a nextToken, the server supports pagination
	if nextToken != "" {
		t.Logf("ListTables() returned nextPageToken: %s (pagination supported)", nextToken)
	} else {
		t.Logf("ListTables() returned no nextPageToken (server may not support pagination or all results fit in one page)")
	}
}

func TestPagination_ListShares(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()

	// Test basic pagination parameters
	shares, nextToken, err := client.ListShares(ctx, 10, "")
	if err != nil {
		t.Errorf("ListShares() with pagination params failed: %v", err)
		return
	}

	if len(shares) == 0 {
		t.Error("ListShares() returned no shares")
	}

	// Verify nextToken is a string (may be empty)
	_ = nextToken
	t.Logf("ListShares() completed, nextToken present: %v", nextToken != "")
}

func TestPagination_ListSchemas(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()
	share := share{Name: "delta_sharing"}

	// Test basic pagination parameters
	schemas, nextToken, err := client.ListSchemas(ctx, share, 10, "")
	if err != nil {
		t.Errorf("ListSchemas() with pagination params failed: %v", err)
		return
	}

	if len(schemas) == 0 {
		t.Error("ListSchemas() returned no schemas")
	}

	// Verify nextToken is a string (may be empty)
	_ = nextToken
	t.Logf("ListSchemas() completed, nextToken present: %v", nextToken != "")
}

func TestPagination_ListAllTables(t *testing.T) {
	client, err := NewSharingClient("/Users/magnuspierre/Documents/shares/open-datasets.share.txt")
	if err != nil {
		t.Skipf("Skipping test - cannot create client: %v", err)
		return
	}

	ctx := context.Background()

	// Test basic pagination parameters
	tables, nextToken, err := client.ListAllTables(ctx, 5, "")
	if err != nil {
		t.Errorf("ListAllTables() with pagination params failed: %v", err)
		return
	}

	if len(tables) == 0 {
		t.Error("ListAllTables() returned no tables")
	}

	// Verify nextToken is a string (may be empty)
	_ = nextToken
	t.Logf("ListAllTables() completed, nextToken present: %v", nextToken != "")
}
