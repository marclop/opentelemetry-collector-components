// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ratelimitprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/gubernator-io/gubernator/v2"
	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"
	"go.uber.org/zap"
)

// guberStore persists gubernator cache items into an Olric DMap using compact
// binary encoding for high performance and low allocation overhead.
type guberStore struct {
	cfg               *config.Config
	daemon            *olric.Olric
	db                olric.DMap
	logger            *zap.Logger
	minDurationMillis int64
	keyName           string
	ctx               context.Context
	cancel            context.CancelFunc

	mu      sync.RWMutex
	started chan struct{}
}

// newGuberStore creates a new store backed by an embedded Olric instance.
func newGuberStore(logger *zap.Logger,
	threshold time.Duration,
	keyName string,
	peers []string,
) (*guberStore, error) {
	cfg := config.New("lan")
	// Update both the Olric config and the standard library's global logger.
	// There are some network calls from Olric that use the standard library's
	// global logger.
	cfg.LogOutput = newZapOlricWriter(logger)
	cfg.LogLevel = logger.Level().CapitalString()
	// Increase the replica count to 3 for better fault tolerance.
	cfg.ReplicaCount = 3
	cfg.Peers = peers
	c := make(chan struct{})
	cfg.Started = func() { close(c) }
	ctx, cancel := context.WithCancel(context.Background())
	return &guberStore{
		logger:            logger,
		cfg:               cfg,
		minDurationMillis: int64(threshold.Milliseconds()),
		started:           c,
		keyName:           keyName,
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// Start starts the Olric daemon and waits for it to be ready.
func (s *guberStore) Start(ctx context.Context) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if the store has already been started. If so, return early.
	select {
	case <-s.started:
		return nil
	default:
	}
	if s.daemon, err = olric.New(s.cfg); err != nil {
		return fmt.Errorf("failed to create olric instance: %w", err)
	}

	errChan := make(chan error)
	go func() {
		defer close(errChan)
		if err := s.daemon.Start(); err != nil {
			err2 := s.daemon.Shutdown(ctx) // Per olric's Godoc recommendation.
			errChan <- errors.Join(
				fmt.Errorf("failed to start gubernator store: %w", err), err2,
			)
		}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: while waiting for olric to start", ctx.Err())
	case <-s.started:
	case err := <-errChan:
		// Channel may be closed by the defer statement in the goroutine before
		// s.started is closed.
		if err != nil {
			return err
		}
	}
	s.db, err = s.daemon.NewEmbeddedClient().NewDMap("gubernator")
	if err != nil {
		return fmt.Errorf("failed to create olric dmap: %w", err)
	}
	return nil
}

// Shutdown shuts down the Olric daemon.
func (s *guberStore) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.started:
		s.cancel()
	default:
		return fmt.Errorf("gubernator store not started")
	}
	return s.daemon.Shutdown(ctx)
}

// gubernator.Store API

// OnChange is called after a rate limit item is updated; it stores the item
// with an absolute PXAT expiration derived from item.ExpireAt (ms epoch).
func (s *guberStore) OnChange(ctx context.Context,
	r *gubernator.RateLimitReq, item *gubernator.CacheItem,
) {
	// If the duration is less than 1 minute, don't store the item.
	if r.Duration < s.minDurationMillis {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.put(ctx, item); err != nil {
		s.logger.Error("failed to put item into olric dmap",
			zap.Error(err),
			zap.String("key", item.Key),
			zap.Int64("expire_at", item.ExpireAt),
		)
	}
}

func (s *guberStore) put(ctx context.Context, ci *gubernator.CacheItem) error {
	buf, err := encodeCacheItem(ci)
	if err != nil {
		return fmt.Errorf("failed to encode cache item: %w", err)
	}
	// Only put the ci if it has an expiration time.
	if ci.ExpireAt > 0 {
		if err := s.db.Put(ctx, ci.Key, buf, olric.PXAT(
			time.Duration(ci.ExpireAt)*time.Millisecond,
		)); err != nil {
			return fmt.Errorf("failed to put item into olric dmap: %w", err)
		}
	}
	return nil
}

// Get loads a cache item by the request HashKey and decodes it from binary.
func (s *guberStore) Get(ctx context.Context, r *gubernator.RateLimitReq) (*gubernator.CacheItem, bool) {
	if r.Duration < s.minDurationMillis {
		return nil, false
	}

	s.mu.RLock()
	resp, err := s.db.Get(ctx, r.HashKey())
	if err != nil {
		s.mu.RUnlock()
		return nil, false
	}
	s.mu.RUnlock()

	data, err := resp.Byte()
	if err != nil {
		return nil, false
	}
	item, err := decodeCacheItem(data)
	return item, err == nil
}

// Remove deletes the given key from the underlying DMap.
func (s *guberStore) Remove(ctx context.Context, key string) {
	s.mu.RUnlock()
	defer s.mu.RUnlock()
	s.db.Delete(ctx, key)
}

// gubernator.Loader API

// Load is called by gubernator just before the instance is ready to accept requests. The implementation
// should return a channel gubernator can read to load all rate limits that should be loaded into the
// instance cache. The implementation should close the channel to indicate no more rate limits left to load.
func (s *guberStore) Load() (chan *gubernator.CacheItem, error) {
	ch := make(chan *gubernator.CacheItem, 10)
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter, err := s.db.Scan(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to scan olric dmap: %w", err)
	}
	go func() {
		defer func() {
			iter.Close()
			defer close(ch)
		}()
		for iter.Next() {
			resp, err := s.db.Get(s.ctx, iter.Key())
			if err != nil {
				s.logger.Error("failed to get item from olric dmap",
					zap.Error(err),
					zap.String("key", iter.Key()),
				)
				continue
			}
			data, err := resp.Byte()
			if err != nil {
				s.logger.Error("failed to get byte from olric dmap",
					zap.Error(err),
					zap.String("key", iter.Key()),
				)
				continue
			}
			item, err := decodeCacheItem(data)
			if err != nil {
				s.logger.Error("failed to decode cache item",
					zap.Error(err),
					zap.String("key", iter.Key()),
				)
				continue
			}
			ch <- item
		}
	}()
	return ch, nil
}

// Save is called by gubernator just before the instance is shutdown. The passed channel should be
// read until the channel is closed.
func (s *guberStore) Save(ch chan *gubernator.CacheItem) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var errs []error
	for item := range ch {
		if err := s.put(s.ctx, item); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Encoder/Decoder for cache items.

const (
	magicGuberStore uint32 = 0x4755424e // "GUBN"
	formatVersion   uint8  = 1
)

func encodeCacheItem(item *gubernator.CacheItem) ([]byte, error) {
	if item == nil {
		return nil, fmt.Errorf("nil cache item")
	}
	keyBytes := []byte(item.Key)
	headerLen := 4 + 1 + 1 + 8 + 8 + 4 // magic + ver + alg + expireAt + invalidAt + keyLen
	var valueLen int
	switch item.Algorithm {
	case gubernator.Algorithm_LEAKY_BUCKET:
		valueLen = 8 + 8 + 8 + 8 + 8 // limit,duration,remaining(float64),updatedAt,burst
	case gubernator.Algorithm_TOKEN_BUCKET:
		valueLen = 4 + 8 + 8 + 8 + 8 // status(int32),limit,duration,remaining,createdAt
	default:
		return nil, fmt.Errorf("unsupported algorithm: %v", item.Algorithm)
	}
	buf := make([]byte, headerLen+len(keyBytes)+valueLen)
	o := 0
	binary.BigEndian.PutUint32(buf[o:], magicGuberStore)
	o += 4
	buf[o] = formatVersion
	o++
	buf[o] = byte(item.Algorithm)
	o++
	binary.BigEndian.PutUint64(buf[o:], uint64(item.ExpireAt))
	o += 8
	binary.BigEndian.PutUint64(buf[o:], uint64(item.InvalidAt))
	o += 8
	binary.BigEndian.PutUint32(buf[o:], uint32(len(keyBytes)))
	o += 4
	copy(buf[o:], keyBytes)
	o += len(keyBytes)

	switch item.Algorithm {
	case gubernator.Algorithm_LEAKY_BUCKET:
		val, ok := item.Value.(*gubernator.LeakyBucketItem)
		if !ok || val == nil {
			return nil, fmt.Errorf("invalid LeakyBucketItem value")
		}
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Limit))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Duration))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], math.Float64bits(val.Remaining))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.UpdatedAt))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Burst))
		o += 8
	case gubernator.Algorithm_TOKEN_BUCKET:
		val, ok := item.Value.(*gubernator.TokenBucketItem)
		if !ok || val == nil {
			return nil, fmt.Errorf("invalid TokenBucketItem value")
		}
		binary.BigEndian.PutUint32(buf[o:], uint32(val.Status))
		o += 4
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Limit))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Duration))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.Remaining))
		o += 8
		binary.BigEndian.PutUint64(buf[o:], uint64(val.CreatedAt))
		o += 8
	}

	return buf, nil
}

func decodeCacheItem(b []byte) (*gubernator.CacheItem, error) {
	if len(b) < 4+1+1+8+8+4 {
		return nil, fmt.Errorf("buffer too small")
	}
	o := 0
	if binary.BigEndian.Uint32(b[o:]) != magicGuberStore {
		return nil, fmt.Errorf("invalid magic")
	}
	o += 4
	ver := b[o]
	o++
	if ver != formatVersion {
		return nil, fmt.Errorf("unsupported version: %d", ver)
	}
	alg := gubernator.Algorithm(b[o])
	o++
	expireAt := int64(binary.BigEndian.Uint64(b[o:]))
	o += 8
	invalidAt := int64(binary.BigEndian.Uint64(b[o:]))
	o += 8
	keyLen := int(binary.BigEndian.Uint32(b[o:]))
	o += 4
	if keyLen < 0 || o+keyLen > len(b) {
		return nil, fmt.Errorf("invalid key length")
	}
	key := string(b[o : o+keyLen])
	o += keyLen

	item := &gubernator.CacheItem{
		Algorithm: alg,
		Key:       key,
		ExpireAt:  expireAt,
		InvalidAt: invalidAt,
	}

	switch alg {
	case gubernator.Algorithm_LEAKY_BUCKET:
		if o+8*5 > len(b) {
			return nil, fmt.Errorf("buffer too small for LeakyBucketItem")
		}
		val := &gubernator.LeakyBucketItem{}
		val.Limit = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.Duration = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.Remaining = math.Float64frombits(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.UpdatedAt = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.Burst = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		item.Value = val
	case gubernator.Algorithm_TOKEN_BUCKET:
		if o+4+8*4 > len(b) {
			return nil, fmt.Errorf("buffer too small for TokenBucketItem")
		}
		val := &gubernator.TokenBucketItem{}
		val.Status = gubernator.Status(binary.BigEndian.Uint32(b[o:]))
		o += 4
		val.Limit = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.Duration = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.Remaining = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		val.CreatedAt = int64(binary.BigEndian.Uint64(b[o:]))
		o += 8
		item.Value = val
	default:
		return nil, fmt.Errorf("unsupported algorithm: %v", alg)
	}

	return item, nil
}

// ZapOlricWriter is an adapter that implements io.Writer.
// It is intended to be used as the LogOutput for an Olric config.
// It intercepts writes from Olric's default log.Logger, parses the
// log level from Olric's internal prefix, and writes a
// structured log entry to the provided zap.Logger.
type ZapOlricWriter struct{ logger *zap.Logger }

// newZapOlricWriter creates a new io.Writer that pipes
// standard log output to a zap.Logger.
func newZapOlricWriter(logger *zap.Logger) io.Writer {
	// Add "logger":"olric" field to all logs from this writer
	return &ZapOlricWriter{logger: logger.Named("olric")}
}

// Write implements the io.Writer interface.
func (l *ZapOlricWriter) Write(p []byte) (n int, err error) {
	// log.Logger always appends a newline. We strip it by adjusting the
	// length, which is more performant than bytes.TrimSpace as it
	// avoids allocations and extra checks.
	messageLen := len(p)
	if messageLen == 0 {
		return 0, nil
	}
	if p[messageLen-1] == '\n' {
		messageLen--
	}
	message := p[:messageLen]

	// The message now starts from "[LEVEL]".
	logLine := message[bytes.IndexByte(message, '['):]
	switch logLine[1] {
	case 'I': // [INFO]
		if logLine[4] == 'O' && logLine[5] == ']' { // Check for "[INFO]"
			l.logger.Info(string(logLine[7:])) // 7 = len("[INFO] ")
		} else {
			l.logger.Info(string(logLine)) // Not a recognized prefix
		}
	case 'D': // [DEBUG]
		if logLine[5] == 'G' && logLine[6] == ']' { // Check for "[DEBUG]"
			l.logger.Debug(string(logLine[8:])) // 8 = len("[DEBUG] ")
		} else {
			l.logger.Info(string(logLine)) // Not a recognized prefix
		}
	case 'W': // [WARN]
		if logLine[4] == 'N' && logLine[5] == ']' { // Check for "[WARN]"
			l.logger.Warn(string(logLine[7:])) // 7 = len("[WARN] ")
		} else {
			l.logger.Info(string(logLine)) // Not a recognized prefix
		}
	case 'E': // [ERROR]
		if logLine[5] == 'R' && logLine[6] == ']' { // Check for "[ERROR]"
			l.logger.Error(string(logLine[8:])) // 8 = len("[ERROR] ")
		} else {
			l.logger.Info(string(logLine)) // Not a recognized prefix
		}
	case 'F': // [FATAL]
		if logLine[5] == 'L' && logLine[6] == ']' { // Check for "[FATAL]"
			l.logger.Fatal(string(logLine[8:])) // 8 = len("[FATAL] ")
		} else {
			l.logger.Info(string(logLine)) // Not a recognized prefix
		}
	default:
		// Default to Info for unknown prefixes or messages without a level
		l.logger.Info(string(logLine))
	}
	// Return the original length of p to satisfy the io.Writer interface
	return len(p), nil
}
