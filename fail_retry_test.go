package redis

import (
	"container/ring"
	"sync"
	"testing"
	"time"
)

func Test_maxFailTimeout_IsAvailable(t *testing.T) {
	type fields struct {
		failTimeout      time.Duration
		maxFails         int
		mux              *sync.RWMutex
		failRecord       *ring.Ring
		lastMarkFailTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
		{
			name:   "case1",
			fields: fields(*newMaxFailTimeout(time.Second, 3)),
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &maxFailTimeout{
				failTimeout:      tt.fields.failTimeout,
				maxFails:         tt.fields.maxFails,
				mux:              tt.fields.mux,
				failRecord:       tt.fields.failRecord,
				lastMarkFailTime: tt.fields.lastMarkFailTime,
			}
			if got := this.IsAvailable(); got != tt.want {
				t.Errorf("maxFailTimeout.IsAvailable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_maxFailTimeout_MarkSuccess(t *testing.T) {
	type fields struct {
		failTimeout      time.Duration
		maxFails         int
		mux              *sync.RWMutex
		failRecord       *ring.Ring
		lastMarkFailTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
		{
			name:   "case1",
			fields: fields(*newMaxFailTimeout(time.Second, 3)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &maxFailTimeout{
				failTimeout:      tt.fields.failTimeout,
				maxFails:         tt.fields.maxFails,
				mux:              tt.fields.mux,
				failRecord:       tt.fields.failRecord,
				lastMarkFailTime: tt.fields.lastMarkFailTime,
			}
			this.MarkSuccess()
		})
	}
}

func Test_maxFailTimeout_MarkFail(t *testing.T) {
	type fields struct {
		failTimeout      time.Duration
		maxFails         int
		mux              *sync.RWMutex
		failRecord       *ring.Ring
		lastMarkFailTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
		{
			name:   "case1",
			fields: fields(*newMaxFailTimeout(time.Second, 3)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &maxFailTimeout{
				failTimeout:      tt.fields.failTimeout,
				maxFails:         tt.fields.maxFails,
				mux:              tt.fields.mux,
				failRecord:       tt.fields.failRecord,
				lastMarkFailTime: tt.fields.lastMarkFailTime,
			}
			this.MarkFail()
		})
	}
}

func Test_maxFailTimeout_All(t *testing.T) {
	mft := newMaxFailTimeout(time.Second, 3)

	mft.MarkFail()
	mft.MarkFail()
	mft.MarkFail()

	//case1
	if mft.IsAvailable() {
		t.Error("test all fail")
	}

	time.Sleep(time.Second)

	//case2
	if !mft.IsAvailable() {
		t.Error("test all fail")
	}

	mft.MarkFail()
	mft.MarkFail()
	mft.MarkSuccess()
	mft.MarkFail()

	//case3
	if !mft.IsAvailable() {
		t.Error("test all fail")
	}

	mft.MarkFail()

	//case4
	if !mft.IsAvailable() {
		t.Error("test all fail")
	}

	mft.MarkFail()

	//case5
	if mft.IsAvailable() {
		t.Error("test all fail")
	}

	mft.MarkSuccess()

	//case6
	if mft.IsAvailable() {
		t.Error("test all fail")
	}
}

func Benchmark_IsAvailable(b *testing.B) {
	mft := newMaxFailTimeout(time.Second, 3)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mft.IsAvailable()
		}
	})
}

func Benchmark_MarkSuccess(b *testing.B) {
	mft := newMaxFailTimeout(time.Second, 3)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mft.MarkSuccess()
		}
	})
}

func Benchmark_MarkFail(b *testing.B) {
	mft := newMaxFailTimeout(time.Second, 3)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mft.MarkFail()
		}
	})
}
