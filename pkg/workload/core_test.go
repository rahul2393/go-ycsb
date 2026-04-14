/*
 *
 * core_test.go
 * workload
 *
 * Created by lintao on 2023/7/20 10:49
 * Copyright © 2020-2023 LINTAO. All rights reserved.
 *
 */

package workload

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/magiconair/properties"
)

func workloadPath(t *testing.T, name string) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}

	return filepath.Join(filepath.Dir(filename), "..", "..", "workloads", name)
}

func Test_core_buildKeyName(t *testing.T) {

	type args struct {
		keyNum int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "buildKeyName",
			args: args{keyNum: 227},
			want: "user6284890712318570100",
		},
		{
			name: "buildKeyName1",
			args: args{keyNum: 154},
			want: "user6284898408899967577",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := core{p: properties.MustLoadFiles([]string{workloadPath(t, "workloadc")}, properties.UTF8, false)}
			if got := c.buildKeyName(tt.args.keyNum); got != tt.want {
				t.Errorf("buildKeyName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRequestPartitionRange(t *testing.T) {
	tests := []struct {
		name           string
		lowerBound     int64
		upperBound     int64
		partitionCount int64
		partitionIndex int64
		partitionSize  int64
		wantLower      int64
		wantUpper      int64
	}{
		{
			name:           "disabled",
			lowerBound:     0,
			upperBound:     999,
			partitionCount: 1,
			partitionIndex: 0,
			partitionSize:  0,
			wantLower:      0,
			wantUpper:      999,
		},
		{
			name:           "contiguous partition",
			lowerBound:     0,
			upperBound:     999,
			partitionCount: 4,
			partitionIndex: 1,
			partitionSize:  0,
			wantLower:      250,
			wantUpper:      499,
		},
		{
			name:           "fixed size shard spread",
			lowerBound:     0,
			upperBound:     1024000000000 - 1,
			partitionCount: 200,
			partitionIndex: 1,
			partitionSize:  100000000,
			wantLower:      5100000000,
			wantUpper:      5199999999,
		},
		{
			name:           "fixed size last shard reaches end",
			lowerBound:     0,
			upperBound:     1024000000000 - 1,
			partitionCount: 200,
			partitionIndex: 199,
			partitionSize:  100000000,
			wantLower:      1023900000000,
			wantUpper:      1023999999999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLower, gotUpper, _ := requestPartitionRange(tt.lowerBound, tt.upperBound, tt.partitionCount, tt.partitionIndex, tt.partitionSize)
			if gotLower != tt.wantLower || gotUpper != tt.wantUpper {
				t.Fatalf("requestPartitionRange() = [%d %d], want [%d %d]", gotLower, gotUpper, tt.wantLower, tt.wantUpper)
			}
		})
	}
}
