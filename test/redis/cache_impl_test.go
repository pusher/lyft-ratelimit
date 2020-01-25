package redis_test

import (
	"testing"

	"github.com/coocood/freecache"

	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v2"
	stats "github.com/lyft/gostats"
	"github.com/lyft/ratelimit/src/config"
	"github.com/lyft/ratelimit/src/redis"

	"math/rand"

	"github.com/golang/mock/gomock"
	"github.com/lyft/ratelimit/test/common"
	mock_redis "github.com/lyft/ratelimit/test/mocks/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	t.Run("WithoutPerSecondRedis", testRedis(false))
	t.Run("WithPerSecondRedis", testRedis(true))
}

func testRedis(usePerSecondRedis bool) func(*testing.T) {
	return func(t *testing.T) {
		assert := assert.New(t)
		controller := gomock.NewController(t)
		defer controller.Finish()

		pool := mock_redis.NewMockPool(controller)
		perSecondPool := mock_redis.NewMockPool(controller)
		timeSource := mock_redis.NewMockTimeSource(controller)
		connection := mock_redis.NewMockConnection(controller)
		perSecondConnection := mock_redis.NewMockConnection(controller)
		response := mock_redis.NewMockResponse(controller)
		var cache redis.RateLimitCache
		statsStore := stats.NewStore(stats.NewNullSink(), false)
		latencyStat := statsStore.Scope("cache")
		if usePerSecondRedis {
			cache = redis.NewRateLimitCacheImpl(pool, perSecondPool, timeSource, rand.New(rand.NewSource(1)), 0, nil, latencyStat)
		} else {
			cache = redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0, nil, latencyStat)
		}

		if usePerSecondRedis {
			perSecondPool.EXPECT().Get().Return(perSecondConnection)
		} else {
			pool.EXPECT().Get().Return(connection)
		}
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		var connUsed *mock_redis.MockConnection
		if usePerSecondRedis {
			connUsed = perSecondConnection
		} else {
			connUsed = connection
		}
		connUsed.EXPECT().PipeAppend("INCRBY", "domain_key_value_1234", uint32(1))
		connUsed.EXPECT().PipeAppend("EXPIRE", "domain_key_value_1234", int64(1))
		connUsed.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(5))
		connUsed.EXPECT().PipeResponse()
		if usePerSecondRedis {
			perSecondPool.EXPECT().Put(perSecondConnection)
		} else {
			pool.EXPECT().Put(connection)
		}

		request := common.NewRateLimitRequest("domain", [][][2]string{{{"key", "value"}}}, 1)
		limits := []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, "key_value", statsStore)}

		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 5}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

		pool.EXPECT().Get().Return(connection)
		timeSource.EXPECT().UnixNow().Return(int64(1234))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key2_value2_subkey2_subvalue2_1200", uint32(1))
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key2_value2_subkey2_subvalue2_1200", int64(60))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(11))
		connection.EXPECT().PipeResponse()
		pool.EXPECT().Put(connection)

		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key2", "value2"}},
				{{"key2", "value2"}, {"subkey2", "subvalue2"}},
			}, 1)
		limits = []*config.RateLimit{
			nil,
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_MINUTE, "key2_value2_subkey2_subvalue2", statsStore)}
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: nil, LimitRemaining: 0},
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[1].Limit, LimitRemaining: 0}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[1].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[1].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[1].Stats.NearLimit.Value())

		pool.EXPECT().Get().Return(connection)
		timeSource.EXPECT().UnixNow().Return(int64(1000000))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_997200", uint32(1))
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_997200", int64(3600))
		connection.EXPECT().PipeAppend("INCRBY", "domain_key3_value3_subkey3_subvalue3_950400", uint32(1))
		connection.EXPECT().PipeAppend(
			"EXPIRE", "domain_key3_value3_subkey3_subvalue3_950400", int64(86400))
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(11))
		connection.EXPECT().PipeResponse()
		connection.EXPECT().PipeResponse().Return(response)
		response.EXPECT().Int().Return(int64(13))
		connection.EXPECT().PipeResponse()
		pool.EXPECT().Put(connection)

		request = common.NewRateLimitRequest(
			"domain",
			[][][2]string{
				{{"key3", "value3"}},
				{{"key3", "value3"}, {"subkey3", "subvalue3"}},
			}, 1)
		limits = []*config.RateLimit{
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_HOUR, "key3_value3", statsStore),
			config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_DAY, "key3_value3_subkey3_subvalue3", statsStore)}
		assert.Equal(
			[]*pb.RateLimitResponse_DescriptorStatus{
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0},
				{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[1].Limit, LimitRemaining: 0}},
			cache.DoLimit(nil, request, limits))
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
		assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
		assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
		assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
	}
}

func testLocalCacheStats(localCacheStats stats.StatGenerator, statsStore stats.Store, sink *common.TestStatSink,
	expectedHitCount int, expectedMissCount int, expectedLookUpCount int, expectedExpiredCount int,
	expectedEntryCount int) func(*testing.T) {
	return func(t *testing.T) {
		localCacheStats.GenerateStats()
		statsStore.Flush()

		// Check whether all local_cache related stats are available.
		_, ok := sink.Record["averageAccessTime"]
		assert.Equal(t, true, ok)
		hitCount, ok := sink.Record["hitCount"]
		assert.Equal(t, true, ok)
		missCount, ok := sink.Record["missCount"]
		assert.Equal(t, true, ok)
		lookupCount, ok := sink.Record["lookupCount"]
		assert.Equal(t, true, ok)
		_, ok = sink.Record["overwriteCount"]
		assert.Equal(t, true, ok)
		_, ok = sink.Record["evacuateCount"]
		assert.Equal(t, true, ok)
		expiredCount, ok := sink.Record["expiredCount"]
		assert.Equal(t, true, ok)
		entryCount, ok := sink.Record["entryCount"]
		assert.Equal(t, true, ok)

		// Check the correctness of hitCount, missCount, lookupCount, expiredCount and entryCount
		assert.Equal(t, expectedHitCount, hitCount.(int))
		assert.Equal(t, expectedMissCount, missCount.(int))
		assert.Equal(t, expectedLookUpCount, lookupCount.(int))
		assert.Equal(t, expectedExpiredCount, expiredCount.(int))
		assert.Equal(t, expectedEntryCount, entryCount.(int))

		sink.Clear()
	}
}

func TestOverLimitWithLocalCache(t *testing.T) {
	assert := assert.New(t)
	controller := gomock.NewController(t)
	defer controller.Finish()

	pool := mock_redis.NewMockPool(controller)
	timeSource := mock_redis.NewMockTimeSource(controller)
	connection := mock_redis.NewMockConnection(controller)
	response := mock_redis.NewMockResponse(controller)
	localCache := freecache.NewCache(100)
	sink := &common.TestStatSink{}
	sink.Clear()
	statsStore := stats.NewStore(sink, true)
	latencyStat := statsStore.Scope("cache")
	cache := redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0, localCache, latencyStat)
	localCacheStats := redis.NewLocalCacheStats(localCache, statsStore.Scope("localcache"))

	// Test Near Limit Stats. Under Near Limit Ratio
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(11))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request := common.NewRateLimitRequest("domain", [][][2]string{{{"key4", "value4"}}}, 1)

	limits := []*config.RateLimit{
		config.NewRateLimit(15, pb.RateLimitResponse_RateLimit_HOUR, "key4_value4", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 4}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimitWithLocalCache.Value())
	assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

	// Check the local cache stats.
	testLocalCacheStats(localCacheStats, statsStore, sink, 0, 1, 1, 0, 0)

	// Test Near Limit Stats. At Near Limit Ratio, still OK
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(13))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 2}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(2), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimitWithLocalCache.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Check the local cache stats.
	testLocalCacheStats(localCacheStats, statsStore, sink, 0, 2, 2, 0, 0)

	// Test Over limit stats
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(16))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimitWithLocalCache.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Check the local cache stats.
	testLocalCacheStats(localCacheStats, statsStore, sink, 0, 2, 3, 0, 1)

	// Test Over limit stats with local cache
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(4), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(2), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.OverLimitWithLocalCache.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Check the local cache stats.
	testLocalCacheStats(localCacheStats, statsStore, sink, 1, 3, 4, 0, 1)
}

func TestNearLimit(t *testing.T) {
	assert := assert.New(t)
	controller := gomock.NewController(t)
	defer controller.Finish()

	pool := mock_redis.NewMockPool(controller)
	timeSource := mock_redis.NewMockTimeSource(controller)
	connection := mock_redis.NewMockConnection(controller)
	response := mock_redis.NewMockResponse(controller)
	statsStore := stats.NewStore(stats.NewNullSink(), false)
	latencyStat := statsStore.Scope("cache")
	cache := redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(rand.NewSource(1)), 0, nil, latencyStat)

	// Test Near Limit Stats. Under Near Limit Ratio
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(11))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request := common.NewRateLimitRequest("domain", [][][2]string{{{"key4", "value4"}}}, 1)

	limits := []*config.RateLimit{
		config.NewRateLimit(15, pb.RateLimitResponse_RateLimit_HOUR, "key4_value4", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 4}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

	// Test Near Limit Stats. At Near Limit Ratio, still OK
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(13))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 2}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(2), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Test Near Limit Stats. We went OVER_LIMIT, but the near_limit counter only increases
	// when we are near limit, not after we have passed the limit.
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1000000))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key4_value4_997200", uint32(1))
	connection.EXPECT().PipeAppend(
		"EXPIRE", "domain_key4_value4_997200", int64(3600))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(16))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{
			{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(1), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Now test hitsAddend that is greater than 1
	// All of it under limit, under near limit
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key5_value5_1234", uint32(3))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key5_value5_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(5))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key5", "value5"}}}, 3)
	limits = []*config.RateLimit{config.NewRateLimit(20, pb.RateLimitResponse_RateLimit_SECOND, "key5_value5", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 15}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())

	// All of it under limit, some over near limit
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key6_value6_1234", uint32(2))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key6_value6_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(7))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key6", "value6"}}}, 2)
	limits = []*config.RateLimit{config.NewRateLimit(8, pb.RateLimitResponse_RateLimit_SECOND, "key6_value6", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 1}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(2), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// All of it under limit, all of it over near limit
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key7_value7_1234", uint32(3))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key7_value7_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(19))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key7", "value7"}}}, 3)
	limits = []*config.RateLimit{config.NewRateLimit(20, pb.RateLimitResponse_RateLimit_SECOND, "key7_value7", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 1}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(3), limits[0].Stats.NearLimit.Value())

	// Some of it over limit, all of it over near limit
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key8_value8_1234", uint32(3))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key8_value8_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(22))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key8", "value8"}}}, 3)
	limits = []*config.RateLimit{config.NewRateLimit(20, pb.RateLimitResponse_RateLimit_SECOND, "key8_value8", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(2), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(1), limits[0].Stats.NearLimit.Value())

	// Some of it in all three places
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key9_value9_1234", uint32(7))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key9_value9_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(22))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key9", "value9"}}}, 7)
	limits = []*config.RateLimit{config.NewRateLimit(20, pb.RateLimitResponse_RateLimit_SECOND, "key9_value9", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(7), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(2), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(4), limits[0].Stats.NearLimit.Value())

	// all of it over limit
	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key10_value10_1234", uint32(3))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key10_value10_1234", int64(1))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(30))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request = common.NewRateLimitRequest("domain", [][][2]string{{{"key10", "value10"}}}, 3)
	limits = []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, "key10_value10", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OVER_LIMIT, CurrentLimit: limits[0].Limit, LimitRemaining: 0}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(3), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(3), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
}

func TestRedisWithJitter(t *testing.T) {
	assert := assert.New(t)
	controller := gomock.NewController(t)
	defer controller.Finish()

	pool := mock_redis.NewMockPool(controller)
	timeSource := mock_redis.NewMockTimeSource(controller)
	connection := mock_redis.NewMockConnection(controller)
	response := mock_redis.NewMockResponse(controller)
	jitterSource := mock_redis.NewMockJitterRandSource(controller)
	statsStore := stats.NewStore(stats.NewNullSink(), false)
	latencyStat := statsStore.Scope("cache")
	cache := redis.NewRateLimitCacheImpl(pool, nil, timeSource, rand.New(jitterSource), 3600, nil, latencyStat)

	pool.EXPECT().Get().Return(connection)
	timeSource.EXPECT().UnixNow().Return(int64(1234))
	jitterSource.EXPECT().Int63().Return(int64(100))
	connection.EXPECT().PipeAppend("INCRBY", "domain_key_value_1234", uint32(1))
	connection.EXPECT().PipeAppend("EXPIRE", "domain_key_value_1234", int64(101))
	connection.EXPECT().PipeResponse().Return(response)
	response.EXPECT().Int().Return(int64(5))
	connection.EXPECT().PipeResponse()
	pool.EXPECT().Put(connection)

	request := common.NewRateLimitRequest("domain", [][][2]string{{{"key", "value"}}}, 1)
	limits := []*config.RateLimit{config.NewRateLimit(10, pb.RateLimitResponse_RateLimit_SECOND, "key_value", statsStore)}

	assert.Equal(
		[]*pb.RateLimitResponse_DescriptorStatus{{Code: pb.RateLimitResponse_OK, CurrentLimit: limits[0].Limit, LimitRemaining: 5}},
		cache.DoLimit(nil, request, limits))
	assert.Equal(uint64(1), limits[0].Stats.TotalHits.Value())
	assert.Equal(uint64(0), limits[0].Stats.OverLimit.Value())
	assert.Equal(uint64(0), limits[0].Stats.NearLimit.Value())
}
