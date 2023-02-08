/*
 * TenantCache.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <limits>
#include <string>

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Tenant.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/TenantCache.h"
#include "flow/flow.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class TenantCacheImpl {

	ACTOR static Future<std::vector<std::pair<int64_t, TenantMapEntry>>> getTenantList(TenantCache* tenantCache,
	                                                                                   Transaction* tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantList =
		    wait(TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
		ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

		return tenantList.results;
	}

public:
	ACTOR static Future<Void> build(TenantCache* tenantCache) {
		state Transaction tr(tenantCache->dbcx());

		TraceEvent(SevInfo, "BuildingTenantCache", tenantCache->id()).log();

		try {
			state std::vector<std::pair<int64_t, TenantMapEntry>> tenantList = wait(getTenantList(tenantCache, &tr));

			for (int i = 0; i < tenantList.size(); i++) {
				tenantCache->insert(tenantList[i].second);

				TraceEvent(SevInfo, "TenantFound", tenantCache->id())
				    .detail("TenantName", tenantList[i].second.tenantName)
				    .detail("TenantID", tenantList[i].first)
				    .detail("TenantPrefix", tenantList[i].second.prefix);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}

		TraceEvent(SevInfo, "BuiltTenantCache", tenantCache->id()).log();

		return Void();
	}

	ACTOR static Future<Void> monitorTenantMap(TenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingTenantCacheMonitor", tenantCache->id()).log();

		state Transaction tr(tenantCache->dbcx());

		state double lastTenantListFetchTime = now();

		loop {
			try {
				if (now() - lastTenantListFetchTime > (2 * SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL)) {
					TraceEvent(SevWarn, "TenantListRefreshDelay", tenantCache->id()).log();
				}

				state std::vector<std::pair<int64_t, TenantMapEntry>> tenantList =
				    wait(getTenantList(tenantCache, &tr));

				tenantCache->startRefresh();
				bool tenantListUpdated = false;

				for (int i = 0; i < tenantList.size(); i++) {
					if (tenantCache->update(tenantList[i].second)) {
						tenantListUpdated = true;
						TenantCacheTenantCreated req(tenantList[i].second.prefix);
						tenantCache->tenantCreationSignal.send(req);
					}
				}

				if (tenantCache->cleanup()) {
					tenantListUpdated = true;
				}

				if (tenantListUpdated) {
					TraceEvent(SevInfo, "TenantCache", tenantCache->id()).detail("List", tenantCache->desc());
				}

				lastTenantListFetchTime = now();
				tr.reset();
				wait(delay(SERVER_KNOBS->TENANT_CACHE_LIST_REFRESH_INTERVAL));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("TenantCacheGetTenantListError", tenantCache->id())
					    .errorUnsuppressed(e)
					    .suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> monitorStorageUsage(TenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingTenantCacheStorageUsageMonitor", tenantCache->id()).log();

		state int refreshInterval = SERVER_KNOBS->TENANT_CACHE_STORAGE_USAGE_REFRESH_INTERVAL;
		state double lastTenantListFetchTime = now();
		state double lastTraceTime = 0;

		loop {
			state double fetchStartTime = now();

			state bool toTrace = false;
			if (fetchStartTime - lastTraceTime > SERVER_KNOBS->TENANT_CACHE_STORAGE_USAGE_TRACE_INTERVAL) {
				toTrace = true;
				lastTraceTime = fetchStartTime;
			}

			state std::vector<int64_t> groups;
			for (const auto& [group, storage] : tenantCache->tenantStorageMap) {
				groups.push_back(group);
			}
			state int i;
			for (i = 0; i < groups.size(); i++) {
				state int64_t group = groups[i];
				state int64_t usage = 0;
				// `tenants` needs to be a copy so that the erase (below) or inserts/erases from other
				// functions (when this actor yields) do not interfere with the iteration
				state std::unordered_set<int64_t> tenants = tenantCache->tenantStorageMap[group].tenants;
				state std::unordered_set<int64_t>::iterator iter = tenants.begin();
				for (; iter != tenants.end(); iter++) {
					state int64_t tenantId = *iter;
					state ReadYourWritesTransaction tr(tenantCache->dbcx(), makeReference<Tenant>(tenantId));
					loop {
						try {
							state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
							usage += size;
							break;
						} catch (Error& e) {
							if (e.code() == error_code_tenant_not_found) {
								tenantCache->tenantStorageMap[group].tenants.erase(tenantId);
								break;
							} else {
								TraceEvent("TenantCacheGetStorageUsageError", tenantCache->id()).error(e);
								wait(tr.onError(e));
							}
						}
					}
				}
				tenantCache->tenantStorageMap[group].usage = usage;

				if (toTrace) {
					// Trace the storage used by all tenant groups for visibility.
					TraceEvent(SevInfo, "StorageUsageUpdated", tenantCache->id())
					    .detail("TenantGroup", group)
					    .detail("Quota", tenantCache->tenantStorageMap[group].quota)
					    .detail("Usage", tenantCache->tenantStorageMap[group].usage);
				}
			}

			lastTenantListFetchTime = now();
			if (lastTenantListFetchTime - fetchStartTime > (2 * refreshInterval)) {
				TraceEvent(SevWarn, "TenantCacheGetStorageUsageRefreshSlow", tenantCache->id()).log();
			}
			wait(delay(refreshInterval));
		}
	}

	ACTOR static Future<Void> monitorStorageQuota(TenantCache* tenantCache) {
		TraceEvent(SevInfo, "StartingTenantCacheStorageQuotaMonitor", tenantCache->id()).log();

		state Reference<ReadYourWritesTransaction> tr = tenantCache->dbcx()->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state KeyBackedRangeResult<std::pair<int64_t, int64_t>> currentQuotas =
				    wait(TenantMetadata::storageQuota().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));
				// Reset the quota for all groups; this essentially sets the quota to `max` for groups where the
				// quota might have been cleared (i.e., groups that will not be returned in `getRange` request above).
				// FIXME
				for (auto& [group, storage] : tenantCache->tenantStorageMap) {
					storage.quota = std::numeric_limits<int64_t>::max();
				}
				for (const auto& [groupId, quota] : currentQuotas.results) {
					tenantCache->tenantStorageMap[groupId].quota = quota;
				}
				tr->reset();
				wait(delay(SERVER_KNOBS->TENANT_CACHE_STORAGE_QUOTA_REFRESH_INTERVAL));
			} catch (Error& e) {
				TraceEvent("TenantCacheGetStorageQuotaError", tenantCache->id()).error(e);
				wait(tr->onError(e));
			}
		}
	}
};

void TenantCache::insert(TenantMapEntry& tenant) {
	ASSERT(tenantCache.find(tenant.prefix) == tenantCache.end());

	TenantInfo tenantInfo(tenant.id, Optional<Standalone<StringRef>>());
	tenantCache[tenantInfo.prefix.get()] = makeReference<TCTenantInfo>(tenantInfo);
	tenantCache[tenantInfo.prefix.get()]->updateCacheGeneration(generation);

	if (tenant.tenantGroup.present()) {
		tenantStorageMap[tenant.tenantGroup.get()].tenants.insert(tenant.id);
	}
}

void TenantCache::startRefresh() {
	ASSERT(generation < std::numeric_limits<uint64_t>::max());
	generation++;
}

void TenantCache::keep(TenantMapEntry& tenant) {
	ASSERT(tenantCache.find(tenant.prefix) != tenantCache.end());
	tenantCache[tenant.prefix]->updateCacheGeneration(generation);
}

bool TenantCache::update(TenantMapEntry& tenant) {
	if (tenantCache.find(tenant.prefix) != tenantCache.end()) {
		keep(tenant);
		return false;
	}

	insert(tenant);
	return true;
}

int TenantCache::cleanup() {
	int tenantsRemoved = 0;
	std::vector<Key> keysToErase;

	for (auto& t : tenantCache) {
		ASSERT(t.value->cacheGeneration() <= generation);
		if (t.value->cacheGeneration() != generation) {
			keysToErase.push_back(t.key);
		}
	}

	for (auto& k : keysToErase) {
		tenantCache.erase(k);
		tenantsRemoved++;
	}

	return tenantsRemoved;
}

std::vector<int64_t> TenantCache::getTenantList() const {
	std::vector<int64_t> tenants;
	for (const auto& [prefix, entry] : tenantCache) {
		tenants.push_back(entry->id());
	}
	return tenants;
}

std::string TenantCache::desc() const {
	std::string s("@Generation: ");
	s += std::to_string(generation) + " ";
	int count = 0;
	for (auto& [tenantPrefix, tenant] : tenantCache) {
		if (count) {
			s += ", ";
		}

		s += "ID: " + std::to_string(tenant->id()) + " Prefix: " + tenantPrefix.toString();
		count++;
	}

	return s;
}

bool TenantCache::isTenantKey(KeyRef key) const {
	auto it = tenantCache.lastLessOrEqual(key);
	if (it == tenantCache.end()) {
		return false;
	}

	if (!key.startsWith(it->key)) {
		return false;
	}

	return true;
}

Future<Void> TenantCache::build() {
	return TenantCacheImpl::build(this);
}

Optional<Reference<TCTenantInfo>> TenantCache::tenantOwning(KeyRef key) const {
	auto it = tenantCache.lastLessOrEqual(key);
	if (it == tenantCache.end()) {
		return {};
	}

	if (!key.startsWith(it->key)) {
		return {};
	}

	return it->value;
}

std::unordered_set<int64_t> TenantCache::getTenantsOverQuota() const {
	std::unordered_set<int64_t> tenantsOverQuota;
	for (const auto& [tenantGroup, storage] : tenantStorageMap) {
		if (storage.usage > storage.quota) {
			tenantsOverQuota.insert(storage.tenants.begin(), storage.tenants.end());
		}
	}
	return tenantsOverQuota;
}

Future<Void> TenantCache::monitorTenantMap() {
	return TenantCacheImpl::monitorTenantMap(this);
}

Future<Void> TenantCache::monitorStorageUsage() {
	return TenantCacheImpl::monitorStorageUsage(this);
}

Future<Void> TenantCache::monitorStorageQuota() {
	return TenantCacheImpl::monitorStorageQuota(this);
}

class TenantCacheUnitTest {
public:
	ACTOR static Future<Void> InsertAndTestPresence() {
		wait(Future<Void>(Void()));

		Database cx;
		TenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, tenantName, TenantState::READY);

			tenantCache.insert(tenant);
		}

		for (int i = 0; i < tenantLimit; i++) {
			Key k(format("%d", i));
			ASSERT(tenantCache.isTenantKey(k.withPrefix(TenantAPI::idToPrefix(tenantNumber + (i % tenantCount)))));
			ASSERT(!tenantCache.isTenantKey(k.withPrefix(allKeys.begin)));
			ASSERT(!tenantCache.isTenantKey(k));
		}

		return Void();
	}

	ACTOR static Future<Void> RefreshAndTestPresence() {
		wait(Future<Void>(Void()));

		Database cx;
		TenantCache tenantCache(cx, UID(1, 0));

		constexpr static uint16_t tenantLimit = 64;

		uint16_t tenantCount = deterministicRandom()->randomInt(1, tenantLimit);
		uint16_t tenantNumber = deterministicRandom()->randomInt(0, std::numeric_limits<uint16_t>::max());

		for (uint16_t i = 0; i < tenantCount; i++) {
			TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantNumber + i));
			TenantMapEntry tenant(tenantNumber + i, tenantName, TenantState::READY);

			tenantCache.insert(tenant);
		}

		uint16_t staleTenantFraction = deterministicRandom()->randomInt(1, 8);
		tenantCache.startRefresh();

		int keepCount = 0, removeCount = 0;
		for (int i = 0; i < tenantCount; i++) {
			uint16_t tenantOrdinal = tenantNumber + i;

			if (tenantOrdinal % staleTenantFraction != 0) {
				TenantName tenantName(format("%s_%08d", "ddtc_test_tenant", tenantOrdinal));
				TenantMapEntry tenant(tenantOrdinal, tenantName, TenantState::READY);
				bool newTenant = tenantCache.update(tenant);
				ASSERT(!newTenant);
				keepCount++;
			} else {
				removeCount++;
			}
		}
		int tenantsRemoved = tenantCache.cleanup();
		ASSERT(tenantsRemoved == removeCount);

		int keptCount = 0, removedCount = 0;
		for (int i = 0; i < tenantCount; i++) {
			uint16_t tenantOrdinal = tenantNumber + i;
			Key k(format("%d", i));
			if (tenantOrdinal % staleTenantFraction != 0) {
				ASSERT(tenantCache.isTenantKey(k.withPrefix(TenantAPI::idToPrefix(tenantOrdinal))));
				keptCount++;
			} else {
				ASSERT(!tenantCache.isTenantKey(k.withPrefix(TenantAPI::idToPrefix(tenantOrdinal))));
				removedCount++;
			}
		}

		ASSERT(keepCount == keptCount);
		ASSERT(removeCount == removedCount);

		return Void();
	}
};

TEST_CASE("/TenantCache/InsertAndTestPresence") {
	wait(TenantCacheUnitTest::InsertAndTestPresence());
	return Void();
}

TEST_CASE("/TenantCache/RefreshAndTestPresence") {
	wait(TenantCacheUnitTest::RefreshAndTestPresence());
	return Void();
}
