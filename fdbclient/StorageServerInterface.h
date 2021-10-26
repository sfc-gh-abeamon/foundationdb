/*
 * StorageServerInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDBCLIENT_STORAGESERVERINTERFACE_H
#define FDBCLIENT_STORAGESERVERINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/TSSComparison.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/UnitTest.h"

// Dead code, removed in the next protocol version
struct VersionReply {
	constexpr static FileIdentifier file_identifier = 3;

	Version version;
	VersionReply() = default;
	explicit VersionReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct TenantName {
	constexpr static FileIdentifier file_identifier = 13733271;
	enum class TenantType {
		DEFAULT_TENANT_TYPE = 0,
		VALIDATE_TENANT_TYPE = 1,
		DO_NOT_VALIDATE_TENANT_TYPE = 2
	} uint8_t;

	TenantName() : tenantType(TenantType::DEFAULT_TENANT_TYPE) {}
	TenantName(std::string tenantName) : tenantType(TenantType::VALIDATE_TENANT_TYPE), tenantName(tenantName) {}

	bool isDefaultTenant() const { return tenantType == TenantType::DEFAULT_TENANT_TYPE; }

	bool hasName() const { return tenantType == TenantType::VALIDATE_TENANT_TYPE; }

	std::string getTenantName() const {
		ASSERT(hasName());
		return tenantName;
	}

	static const TenantName DEFAULT;
	static const TenantName DO_NOT_VALIDATE;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantType);
		if (tenantType == TenantType::VALIDATE_TENANT_TYPE) {
			serializer(ar, tenantName);
		}
	}

private:
	TenantName(TenantType tenantType) : tenantType(tenantType) {}

	TenantType tenantType;
	std::string tenantName;
};

template <>
struct Traceable<TenantName> : std::true_type {
	static std::string toString(const TenantName& value) {
		if (value.isDefaultTenant()) {
			return "DefaultTenant";
		} else if (value.hasName()) {
			return "Validate: " + value.getTenantName();
		} else {
			return "DoNotValidate";
		}
	}
};

struct StorageServerInterface {
	constexpr static FileIdentifier file_identifier = 15302073;
	enum { BUSY_ALLOWED = 0, BUSY_FORCE = 1, BUSY_LOCAL = 2 };

	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 0 };

	LocalityData locality;
	UID uniqueID;
	Optional<UID> tssPairID;

	RequestStream<struct GetValueRequest> getValue;
	RequestStream<struct GetKeyRequest> getKey;

	// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
	// selector offset prevents all data from being read in one range read
	RequestStream<struct GetKeyValuesRequest> getKeyValues;

	RequestStream<struct GetShardStateRequest> getShardState;
	RequestStream<struct WaitMetricsRequest> waitMetrics;
	RequestStream<struct SplitMetricsRequest> splitMetrics;
	RequestStream<struct GetStorageMetricsRequest> getStorageMetrics;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct StorageQueuingMetricsRequest> getQueuingMetrics;

	RequestStream<ReplyPromise<KeyValueStoreType>> getKeyValueStoreType;
	RequestStream<struct WatchValueRequest> watchValue;
	RequestStream<struct ReadHotSubRangeRequest> getReadHotRanges;
	RequestStream<struct SplitRangeRequest> getRangeSplitPoints;
	RequestStream<struct GetKeyValuesStreamRequest> getKeyValuesStream;
	RequestStream<struct ChangeFeedStreamRequest> changeFeedStream;
	RequestStream<struct OverlappingChangeFeedsRequest> overlappingChangeFeeds;
	RequestStream<struct ChangeFeedPopRequest> changeFeedPop;

	explicit StorageServerInterface(UID uid) : uniqueID(uid) {}
	StorageServerInterface() : uniqueID(deterministicRandom()->randomUniqueID()) {}
	NetworkAddress address() const { return getValue.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return getValue.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return getValue.getEndpoint().addresses.secondaryAddress; }
	UID id() const { return uniqueID; }
	bool isTss() const { return tssPairID.present(); }
	std::string toString() const { return id().shortString(); }
	template <class Ar>
	void serialize(Ar& ar) {
		// StorageServerInterface is persisted in the database, so changes here have to be versioned carefully!
		// To change this serialization, ProtocolVersion::ServerListValue must be updated, and downgrades need to be
		// considered

		if (ar.protocolVersion().hasSmallEndpoints()) {
			if (ar.protocolVersion().hasTSS()) {
				serializer(ar, uniqueID, locality, getValue, tssPairID);
			} else {
				serializer(ar, uniqueID, locality, getValue);
			}
			if (Ar::isDeserializing) {
				getKey = RequestStream<struct GetKeyRequest>(getValue.getEndpoint().getAdjustedEndpoint(1));
				getKeyValues = RequestStream<struct GetKeyValuesRequest>(getValue.getEndpoint().getAdjustedEndpoint(2));
				getShardState =
				    RequestStream<struct GetShardStateRequest>(getValue.getEndpoint().getAdjustedEndpoint(3));
				waitMetrics = RequestStream<struct WaitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(4));
				splitMetrics = RequestStream<struct SplitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(5));
				getStorageMetrics =
				    RequestStream<struct GetStorageMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(6));
				waitFailure = RequestStream<ReplyPromise<Void>>(getValue.getEndpoint().getAdjustedEndpoint(7));
				getQueuingMetrics =
				    RequestStream<struct StorageQueuingMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(8));
				getKeyValueStoreType =
				    RequestStream<ReplyPromise<KeyValueStoreType>>(getValue.getEndpoint().getAdjustedEndpoint(9));
				watchValue = RequestStream<struct WatchValueRequest>(getValue.getEndpoint().getAdjustedEndpoint(10));
				getReadHotRanges =
				    RequestStream<struct ReadHotSubRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(11));
				getRangeSplitPoints =
				    RequestStream<struct SplitRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(12));
				getKeyValuesStream =
				    RequestStream<struct GetKeyValuesStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(13));
				changeFeedStream =
				    RequestStream<struct ChangeFeedStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(14));
				overlappingChangeFeeds =
				    RequestStream<struct OverlappingChangeFeedsRequest>(getValue.getEndpoint().getAdjustedEndpoint(15));
				changeFeedPop =
				    RequestStream<struct ChangeFeedPopRequest>(getValue.getEndpoint().getAdjustedEndpoint(16));
			}
		} else {
			ASSERT(Ar::isDeserializing);
			if constexpr (is_fb_function<Ar>) {
				ASSERT(false);
			}
			serializer(ar,
			           uniqueID,
			           locality,
			           getValue,
			           getKey,
			           getKeyValues,
			           getShardState,
			           waitMetrics,
			           splitMetrics,
			           getStorageMetrics,
			           waitFailure,
			           getQueuingMetrics,
			           getKeyValueStoreType);
			if (ar.protocolVersion().hasWatches()) {
				serializer(ar, watchValue);
			}
		}
	}
	bool operator==(StorageServerInterface const& s) const { return uniqueID == s.uniqueID; }
	bool operator<(StorageServerInterface const& s) const { return uniqueID < s.uniqueID; }
	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(getValue.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getKey.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getKeyValues.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getShardState.getReceiver());
		streams.push_back(waitMetrics.getReceiver());
		streams.push_back(splitMetrics.getReceiver());
		streams.push_back(getStorageMetrics.getReceiver());
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(getQueuingMetrics.getReceiver());
		streams.push_back(getKeyValueStoreType.getReceiver());
		streams.push_back(watchValue.getReceiver());
		streams.push_back(getReadHotRanges.getReceiver());
		streams.push_back(getRangeSplitPoints.getReceiver());
		streams.push_back(getKeyValuesStream.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(changeFeedStream.getReceiver());
		streams.push_back(overlappingChangeFeeds.getReceiver());
		streams.push_back(changeFeedPop.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct StorageInfo : NonCopyable, public ReferenceCounted<StorageInfo> {
	Tag tag;
	StorageServerInterface interf;
	StorageInfo() : tag(invalidTag) {}
};

struct ServerCacheInfo {
	std::vector<Tag> tags; // all tags in both primary and remote DC for the key-range
	std::vector<Reference<StorageInfo>> src_info;
	std::vector<Reference<StorageInfo>> dest_info;

	void populateTags() {
		if (tags.size())
			return;

		for (const auto& info : src_info) {
			tags.push_back(info->tag);
		}
		for (const auto& info : dest_info) {
			tags.push_back(info->tag);
		}
		uniquify(tags);
	}
};

struct GetValueReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1378929;
	Optional<Value> value;
	bool cached;

	GetValueReply() : cached(false) {}
	GetValueReply(Optional<Value> value, bool cached) : value(value), cached(cached) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, value, cached);
	}
};

struct GetValueRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 8454530;
	SpanID spanContext;
	TenantName tenant;
	Key key;
	Version version;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetValueReply> reply;

	GetValueRequest() {}

	GetValueRequest(SpanID spanContext, const Key& key, Version ver, Optional<TagSet> tags, Optional<UID> debugID)
	  : spanContext(spanContext), tenant(TenantName::DEFAULT), key(key), version(ver), tags(tags), debugID(debugID) {}

	GetValueRequest(SpanID spanContext,
	                const TenantName& tenant,
	                const Key& key,
	                Version ver,
	                Optional<TagSet> tags,
	                Optional<UID> debugID)
	  : spanContext(spanContext), tenant(tenant), key(key), version(ver), tags(tags), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, version, tags, debugID, reply, spanContext, tenant);
	}
};

struct WatchValueReply {
	constexpr static FileIdentifier file_identifier = 3;

	Version version;
	bool cached = false;
	WatchValueReply() = default;
	explicit WatchValueReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, cached);
	}
};

struct WatchValueRequest {
	constexpr static FileIdentifier file_identifier = 14747733;
	SpanID spanContext;
	TenantName tenant;
	Key key;
	Optional<Value> value;
	Version version;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<WatchValueReply> reply;

	WatchValueRequest() {}

	WatchValueRequest(SpanID spanContext,
	                  const Key& key,
	                  Optional<Value> value,
	                  Version ver,
	                  Optional<TagSet> tags,
	                  Optional<UID> debugID)
	  : spanContext(spanContext), tenant(TenantName::DEFAULT), key(key), value(value), version(ver), tags(tags),
	    debugID(debugID) {}

	WatchValueRequest(SpanID spanContext,
	                  const TenantName& tenant,
	                  const Key& key,
	                  Optional<Value> value,
	                  Version ver,
	                  Optional<TagSet> tags,
	                  Optional<UID> debugID)
	  : spanContext(spanContext), tenant(tenant), key(key), value(value), version(ver), tags(tags), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, version, tags, debugID, reply, spanContext, tenant);
	}
};

struct GetKeyValuesReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetKeyValuesReply() : version(invalidVersion), more(false), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, data, version, more, cached, arena);
	}
};

struct GetKeyValuesRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanID spanContext;
	Arena arena;
	TenantName tenant;
	KeySelectorRef begin, end;
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetKeyValuesReply> reply;

	GetKeyValuesRequest() : isFetchKeys(false) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, begin, end, version, limit, limitBytes, isFetchKeys, tags, debugID, reply, spanContext, tenant, arena);
	}
};

struct GetKeyValuesStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetKeyValuesStreamReply() : version(invalidVersion), more(false), cached(false) {}
	GetKeyValuesStreamReply(GetKeyValuesReply r)
	  : arena(r.arena), data(r.data), version(r.version), more(r.more), cached(r.cached) {}

	int expectedSize() const { return sizeof(GetKeyValuesStreamReply) + data.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           ReplyPromiseStreamReply::acknowledgeToken,
		           ReplyPromiseStreamReply::sequence,
		           data,
		           version,
		           more,
		           cached,
		           arena);
	}
};

struct GetKeyValuesStreamRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanID spanContext;
	Arena arena;
	TenantName tenant;
	KeySelectorRef begin, end;
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromiseStream<GetKeyValuesStreamReply> reply;

	GetKeyValuesStreamRequest() : isFetchKeys(false) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, begin, end, version, limit, limitBytes, isFetchKeys, tags, debugID, reply, spanContext, tenant, arena);
	}
};

struct GetKeyReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 11226513;
	KeySelector sel;
	bool cached;

	GetKeyReply() : cached(false) {}
	GetKeyReply(KeySelector sel, bool cached) : sel(sel), cached(cached) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, sel, cached);
	}
};

struct GetKeyRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 10457870;
	SpanID spanContext;
	Arena arena;
	TenantName tenant;
	KeySelectorRef sel;
	Version version; // or latestVersion
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetKeyReply> reply;

	GetKeyRequest() {}

	GetKeyRequest(SpanID spanContext,
	              KeySelectorRef const& sel,
	              Version version,
	              Optional<TagSet> tags,
	              Optional<UID> debugID)
	  : spanContext(spanContext), tenant(TenantName::DEFAULT), sel(sel), version(version), debugID(debugID) {}

	GetKeyRequest(SpanID spanContext,
	              TenantName const& tenant,
	              KeySelectorRef const& sel,
	              Version version,
	              Optional<TagSet> tags,
	              Optional<UID> debugID)
	  : spanContext(spanContext), tenant(tenant), sel(sel), version(version), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sel, version, tags, debugID, reply, spanContext, tenant, arena);
	}
};

struct GetShardStateReply {
	constexpr static FileIdentifier file_identifier = 0;

	Version first;
	Version second;
	GetShardStateReply() = default;
	GetShardStateReply(Version first, Version second) : first(first), second(second) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, first, second);
	}
};

struct GetShardStateRequest {
	constexpr static FileIdentifier file_identifier = 15860168;
	enum waitMode { NO_WAIT = 0, FETCHING = 1, READABLE = 2 };

	KeyRange keys;
	int32_t mode;
	ReplyPromise<GetShardStateReply> reply;
	GetShardStateRequest() {}
	GetShardStateRequest(KeyRange const& keys, waitMode mode) : keys(keys), mode(mode) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, mode, reply);
	}
};

struct StorageMetrics {
	constexpr static FileIdentifier file_identifier = 13622226;
	int64_t bytes = 0; // total storage
	// FIXME: currently, neither of bytesPerKSecond or iosPerKSecond are actually used in DataDistribution calculations.
	// This may change in the future, but this comment is left here to avoid any confusion for the time being.
	int64_t bytesPerKSecond = 0; // network bandwidth (average over 10s)
	int64_t iosPerKSecond = 0;
	int64_t bytesReadPerKSecond = 0;

	static const int64_t infinity = 1LL << 60;

	bool allLessOrEqual(const StorageMetrics& rhs) const {
		return bytes <= rhs.bytes && bytesPerKSecond <= rhs.bytesPerKSecond && iosPerKSecond <= rhs.iosPerKSecond &&
		       bytesReadPerKSecond <= rhs.bytesReadPerKSecond;
	}
	void operator+=(const StorageMetrics& rhs) {
		bytes += rhs.bytes;
		bytesPerKSecond += rhs.bytesPerKSecond;
		iosPerKSecond += rhs.iosPerKSecond;
		bytesReadPerKSecond += rhs.bytesReadPerKSecond;
	}
	void operator-=(const StorageMetrics& rhs) {
		bytes -= rhs.bytes;
		bytesPerKSecond -= rhs.bytesPerKSecond;
		iosPerKSecond -= rhs.iosPerKSecond;
		bytesReadPerKSecond -= rhs.bytesReadPerKSecond;
	}
	template <class F>
	void operator*=(F f) {
		bytes *= f;
		bytesPerKSecond *= f;
		iosPerKSecond *= f;
		bytesReadPerKSecond *= f;
	}
	bool allZero() const { return !bytes && !bytesPerKSecond && !iosPerKSecond && !bytesReadPerKSecond; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bytes, bytesPerKSecond, iosPerKSecond, bytesReadPerKSecond);
	}

	void negate() { operator*=(-1.0); }
	StorageMetrics operator-() const {
		StorageMetrics x(*this);
		x.negate();
		return x;
	}
	StorageMetrics operator+(const StorageMetrics& r) const {
		StorageMetrics x(*this);
		x += r;
		return x;
	}
	StorageMetrics operator-(const StorageMetrics& r) const {
		StorageMetrics x(r);
		x.negate();
		x += *this;
		return x;
	}
	template <class F>
	StorageMetrics operator*(F f) const {
		StorageMetrics x(*this);
		x *= f;
		return x;
	}

	bool operator==(StorageMetrics const& rhs) const {
		return bytes == rhs.bytes && bytesPerKSecond == rhs.bytesPerKSecond && iosPerKSecond == rhs.iosPerKSecond &&
		       bytesReadPerKSecond == rhs.bytesReadPerKSecond;
	}

	std::string toString() const {
		return format("Bytes: %lld, BPerKSec: %lld, iosPerKSec: %lld, BReadPerKSec: %lld",
		              bytes,
		              bytesPerKSecond,
		              iosPerKSecond,
		              bytesReadPerKSecond);
	}
};

struct WaitMetricsRequest {
	// Waits for any of the given minimum or maximum metrics to be exceeded, and then returns the current values
	// Send a reversed range for min, max to receive an immediate report
	constexpr static FileIdentifier file_identifier = 1795961;
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics min, max;
	ReplyPromise<StorageMetrics> reply;

	WaitMetricsRequest() {}
	WaitMetricsRequest(KeyRangeRef const& keys, StorageMetrics const& min, StorageMetrics const& max)
	  : keys(arena, keys), min(min), max(max) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, min, max, reply, arena);
	}
};

struct SplitMetricsReply {
	constexpr static FileIdentifier file_identifier = 11530792;
	Standalone<VectorRef<KeyRef>> splits;
	StorageMetrics used;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splits, used);
	}
};

struct SplitMetricsRequest {
	constexpr static FileIdentifier file_identifier = 10463876;
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics limits;
	StorageMetrics used;
	StorageMetrics estimated;
	bool isLastShard;
	ReplyPromise<SplitMetricsReply> reply;

	SplitMetricsRequest() {}
	SplitMetricsRequest(KeyRangeRef const& keys,
	                    StorageMetrics const& limits,
	                    StorageMetrics const& used,
	                    StorageMetrics const& estimated,
	                    bool isLastShard)
	  : keys(arena, keys), limits(limits), used(used), estimated(estimated), isLastShard(isLastShard) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, limits, used, estimated, isLastShard, reply, arena);
	}
};

// Should always be used inside a `Standalone`.
struct ReadHotRangeWithMetrics {
	KeyRangeRef keys;
	// density refers to the ratio of bytes sent(because of the read) and bytes on disk.
	// For example if key range [A, B) and [B, C) respectively has byte size 100 bytes on disk.
	// Key range [A,B) was read 30 times.
	// The density for key range [A,C) is 30 * 100 / 200 = 15
	double density;
	// How many bytes of data was sent in a period of time because of read requests.
	double readBandwidth;

	ReadHotRangeWithMetrics() = default;
	ReadHotRangeWithMetrics(KeyRangeRef const& keys, double density, double readBandwidth)
	  : keys(keys), density(density), readBandwidth(readBandwidth) {}

	ReadHotRangeWithMetrics(Arena& arena, const ReadHotRangeWithMetrics& rhs)
	  : keys(arena, rhs.keys), density(rhs.density), readBandwidth(rhs.readBandwidth) {}

	int expectedSize() const { return keys.expectedSize() + sizeof(density) + sizeof(readBandwidth); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, density, readBandwidth);
	}
};

struct ReadHotSubRangeReply {
	constexpr static FileIdentifier file_identifier = 10424537;
	Standalone<VectorRef<ReadHotRangeWithMetrics>> readHotRanges;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readHotRanges);
	}
};
struct ReadHotSubRangeRequest {
	constexpr static FileIdentifier file_identifier = 10259266;
	Arena arena;
	KeyRangeRef keys;
	ReplyPromise<ReadHotSubRangeReply> reply;

	ReadHotSubRangeRequest() {}
	ReadHotSubRangeRequest(KeyRangeRef const& keys) : keys(arena, keys) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, reply, arena);
	}
};

struct SplitRangeReply {
	constexpr static FileIdentifier file_identifier = 11813134;
	// If the given range can be divided, contains the split points.
	// If the given range cannot be divided(for exmaple its total size is smaller than the chunk size), this would be
	// empty
	Standalone<VectorRef<KeyRef>> splitPoints;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splitPoints);
	}
};
struct SplitRangeRequest {
	constexpr static FileIdentifier file_identifier = 10725174;
	Arena arena;
	KeyRangeRef keys;
	int64_t chunkSize;
	ReplyPromise<SplitRangeReply> reply;

	SplitRangeRequest() {}
	SplitRangeRequest(KeyRangeRef const& keys, int64_t chunkSize) : keys(arena, keys), chunkSize(chunkSize) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, chunkSize, reply, arena);
	}
};

struct MutationsAndVersionRef {
	VectorRef<MutationRef> mutations;
	Version version = invalidVersion;
	Version knownCommittedVersion = invalidVersion;

	MutationsAndVersionRef() {}
	explicit MutationsAndVersionRef(Version version, Version knownCommittedVersion)
	  : version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(VectorRef<MutationRef> mutations, Version version, Version knownCommittedVersion)
	  : mutations(mutations), version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(Arena& to, VectorRef<MutationRef> mutations, Version version, Version knownCommittedVersion)
	  : mutations(to, mutations), version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(Arena& to, const MutationsAndVersionRef& from)
	  : mutations(to, from.mutations), version(from.version), knownCommittedVersion(from.knownCommittedVersion) {}
	int expectedSize() const { return mutations.expectedSize(); }

	struct OrderByVersion {
		bool operator()(MutationsAndVersionRef const& a, MutationsAndVersionRef const& b) const {
			return a.version < b.version;
		}
	};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutations, version, knownCommittedVersion);
	}
};

struct ChangeFeedStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<MutationsAndVersionRef> mutations;

	ChangeFeedStreamReply() {}

	int expectedSize() const { return sizeof(ChangeFeedStreamReply) + mutations.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence, mutations, arena);
	}
};

struct ChangeFeedStreamRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanID spanContext;
	Arena arena;
	Key rangeID;
	Version begin = 0;
	Version end = 0;
	KeyRange range;
	ReplyPromiseStream<ChangeFeedStreamReply> reply;

	ChangeFeedStreamRequest() {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeID, begin, end, range, reply, spanContext, arena);
	}
};

struct ChangeFeedPopRequest {
	constexpr static FileIdentifier file_identifier = 10726174;
	Key rangeID;
	Version version;
	KeyRange range;
	ReplyPromise<Void> reply;

	ChangeFeedPopRequest() {}
	ChangeFeedPopRequest(Key const& rangeID, Version version, KeyRange const& range)
	  : rangeID(rangeID), version(version), range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeID, version, range, reply);
	}
};

struct OverlappingChangeFeedEntry {
	Key rangeId;
	KeyRange range;
	bool stopped = false;

	bool operator==(const OverlappingChangeFeedEntry& r) const {
		return rangeId == r.rangeId && range == r.range && stopped == r.stopped;
	}

	OverlappingChangeFeedEntry() {}
	OverlappingChangeFeedEntry(Key const& rangeId, KeyRange const& range, bool stopped)
	  : rangeId(rangeId), range(range), stopped(stopped) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeId, range, stopped);
	}
};

struct OverlappingChangeFeedsReply {
	constexpr static FileIdentifier file_identifier = 11815134;
	std::vector<OverlappingChangeFeedEntry> rangeIds;
	bool cached;
	Arena arena;

	OverlappingChangeFeedsReply() : cached(false) {}
	explicit OverlappingChangeFeedsReply(std::vector<OverlappingChangeFeedEntry> const& rangeIds)
	  : rangeIds(rangeIds), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeIds, arena);
	}
};

struct OverlappingChangeFeedsRequest {
	constexpr static FileIdentifier file_identifier = 10726174;
	KeyRange range;
	Version minVersion;
	ReplyPromise<OverlappingChangeFeedsReply> reply;

	OverlappingChangeFeedsRequest() {}
	explicit OverlappingChangeFeedsRequest(KeyRange const& range) : range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, minVersion, reply);
	}
};

struct GetStorageMetricsReply {
	constexpr static FileIdentifier file_identifier = 15491478;
	StorageMetrics load;
	StorageMetrics available;
	StorageMetrics capacity;
	double bytesInputRate;
	int64_t versionLag;
	double lastUpdate;

	GetStorageMetricsReply() : bytesInputRate(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, load, available, capacity, bytesInputRate, versionLag, lastUpdate);
	}
};

struct GetStorageMetricsRequest {
	constexpr static FileIdentifier file_identifier = 13290999;
	ReplyPromise<GetStorageMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct StorageQueuingMetricsReply {
	constexpr static FileIdentifier file_identifier = 7633366;
	double localTime;
	int64_t instanceID; // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable, bytesInput;
	StorageBytes storageBytes;
	Version version; // current storage server version
	Version durableVersion; // latest version durable on storage server
	double cpuUsage;
	double diskUsage;
	double localRateLimit;
	Optional<TransactionTag> busiestTag;
	double busiestTagFractionalBusyness;
	double busiestTagRate;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           localTime,
		           instanceID,
		           bytesDurable,
		           bytesInput,
		           version,
		           storageBytes,
		           durableVersion,
		           cpuUsage,
		           diskUsage,
		           localRateLimit,
		           busiestTag,
		           busiestTagFractionalBusyness,
		           busiestTagRate);
	}
};

struct StorageQueuingMetricsRequest {
	// SOMEDAY: Send threshold value to avoid polling faster than the information changes?
	constexpr static FileIdentifier file_identifier = 3978640;
	ReplyPromise<struct StorageQueuingMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

#endif
