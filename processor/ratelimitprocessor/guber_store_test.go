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
	"testing"

	"github.com/gubernator-io/gubernator/v2"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode_LeakyBucket_RoundTrip(t *testing.T) {
	item := &gubernator.CacheItem{
		Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
		Key:       "leaky-key",
		ExpireAt:  1730000000123,
		InvalidAt: 1730000000456,
		Value: &gubernator.LeakyBucketItem{
			Limit:     1000,
			Duration:  1000,
			Remaining: 123.5,
			UpdatedAt: 1730000000678,
			Burst:     1500,
		},
	}

	buf, err := encodeCacheItem(item)
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	back, err := decodeCacheItem(buf)
	require.NoError(t, err)
	require.NotNil(t, back)

	require.Equal(t, item.Algorithm, back.Algorithm)
	require.Equal(t, item.Key, back.Key)
	require.Equal(t, item.ExpireAt, back.ExpireAt)
	require.Equal(t, item.InvalidAt, back.InvalidAt)

	got, ok := back.Value.(*gubernator.LeakyBucketItem)
	require.True(t, ok)
	want := item.Value.(*gubernator.LeakyBucketItem)
	require.Equal(t, want.Limit, got.Limit)
	require.Equal(t, want.Duration, got.Duration)
	require.Equal(t, want.Remaining, got.Remaining)
	require.Equal(t, want.UpdatedAt, got.UpdatedAt)
	require.Equal(t, want.Burst, got.Burst)
}

func TestEncodeDecode_TokenBucket_RoundTrip(t *testing.T) {
	item := &gubernator.CacheItem{
		Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
		Key:       "token-key",
		ExpireAt:  1731000000123,
		InvalidAt: 1731000000456,
		Value: &gubernator.TokenBucketItem{
			Status:    gubernator.Status_OVER_LIMIT,
			Limit:     2500,
			Duration:  2000,
			Remaining: 42,
			CreatedAt: 1731000000789,
		},
	}

	buf, err := encodeCacheItem(item)
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	back, err := decodeCacheItem(buf)
	require.NoError(t, err)
	require.NotNil(t, back)

	require.Equal(t, item.Algorithm, back.Algorithm)
	require.Equal(t, item.Key, back.Key)
	require.Equal(t, item.ExpireAt, back.ExpireAt)
	require.Equal(t, item.InvalidAt, back.InvalidAt)

	got, ok := back.Value.(*gubernator.TokenBucketItem)
	require.True(t, ok)
	want := item.Value.(*gubernator.TokenBucketItem)
	require.Equal(t, want.Status, got.Status)
	require.Equal(t, want.Limit, got.Limit)
	require.Equal(t, want.Duration, got.Duration)
	require.Equal(t, want.Remaining, got.Remaining)
	require.Equal(t, want.CreatedAt, got.CreatedAt)
}

func TestDecode_Errors(t *testing.T) {
	// too small
	_, err := decodeCacheItem([]byte{0x00})
	require.Error(t, err)

	// invalid magic
	item := &gubernator.CacheItem{
		Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
		Key:       "k",
		Value: &gubernator.TokenBucketItem{
			Status:    gubernator.Status_UNDER_LIMIT,
			Limit:     1,
			Duration:  1,
			Remaining: 1,
			CreatedAt: 1,
		},
	}
	buf, err := encodeCacheItem(item)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(buf), 5)
	buf[0] = 0x00 // break magic
	_, err = decodeCacheItem(buf)
	require.Error(t, err)

	// wrong version
	buf, err = encodeCacheItem(item)
	require.NoError(t, err)
	buf[4] = 0xFF // unsupported version
	_, err = decodeCacheItem(buf)
	require.Error(t, err)
}
