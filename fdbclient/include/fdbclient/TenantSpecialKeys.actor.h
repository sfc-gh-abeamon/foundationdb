/*
 * TenantSpecialKeys.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_G_H)
#define FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_G_H
#include "fdbclient/TenantSpecialKeys.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_H)
#define FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_H

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Tuple.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class TenantRangeImpl : public SpecialKeyRangeRWImpl {
private:
	static KeyRangeRef removePrefix(KeyRangeRef range, KeyRef prefix, KeyRef defaultEnd) {
		KeyRef begin = range.begin.removePrefix(prefix);
		KeyRef end;
		if (range.end.startsWith(prefix)) {
			end = range.end.removePrefix(prefix);
		} else {
			end = defaultEnd;
		}

		return KeyRangeRef(begin, end);
	}

	static KeyRef withTenantMapPrefix(KeyRef key, Arena& ar) {
		int keySize = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.size() +
		              submoduleRange.begin.size() + mapSubRange.begin.size() + key.size();

		KeyRef prefixedKey = makeString(keySize, ar);
		uint8_t* mutableKey = mutateString(prefixedKey);

		mutableKey = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.copyTo(mutableKey);
		mutableKey = submoduleRange.begin.copyTo(mutableKey);
		mutableKey = mapSubRange.begin.copyTo(mutableKey);

		key.copyTo(mutableKey);
		return prefixedKey;
	}

	ACTOR static Future<Void> getTenantList(ReadYourWritesTransaction* ryw,
	                                        KeyRangeRef kr,
	                                        RangeResult* results,
	                                        GetRangeLimits limitsHint) {
		std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
		    wait(TenantAPI::listTenantMetadataTransaction(&ryw->getTransaction(), kr.begin, kr.end, limitsHint.rows));

		for (auto tenant : tenants) {
			std::string jsonString = tenant.second.toJson();
			ValueRef tenantEntryBytes(results->arena(), jsonString);
			results->push_back(results->arena(),
			                   KeyValueRef(withTenantMapPrefix(tenant.first, results->arena()), tenantEntryBytes));
		}

		return Void();
	}

	ACTOR static Future<RangeResult> getTenantRange(ReadYourWritesTransaction* ryw,
	                                                KeyRangeRef kr,
	                                                GetRangeLimits limitsHint) {
		state RangeResult results;

		kr = kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
		         .removePrefix(TenantRangeImpl::submoduleRange.begin);

		if (kr.intersects(TenantRangeImpl::mapSubRange)) {
			GetRangeLimits limits = limitsHint;
			limits.decrement(results);
			wait(getTenantList(
			    ryw,
			    removePrefix(kr & TenantRangeImpl::mapSubRange, TenantRangeImpl::mapSubRange.begin, "\xff"_sr),
			    &results,
			    limits));
		}

		return results;
	}

	// Returns true if the tenant was created, false if it already existed
	ACTOR static Future<bool> createTenant(
	    ReadYourWritesTransaction* ryw,
	    TenantNameRef tenantName,
	    std::vector<std::pair<Standalone<StringRef>, Optional<Value>>> configMutations,
	    int64_t tenantId) {
		state TenantMapEntry tenantEntry(tenantId, tenantName, TenantState::READY);

		Optional<TenantGroupName> tenantGroup;
		state std::vector<std::pair<Standalone<StringRef>, Optional<Value>>>::iterator configItr;
		for (configItr = configMutations.begin(); configItr != configMutations.end(); ++configItr) {
			if (configItr->first == "tenant_group"_sr && configItr->second.present()) {
				Optional<int64_t> groupId =
				    wait(TenantMetadata::tenantGroupNameIndex().get(&ryw->getTransaction(), configItr->second.get()));
				if (!groupId.present()) {
					throw tenant_group_not_found();
				}
				tenantEntry.tenantGroup = groupId.get();
			}
			tenantEntry.configure(configItr->first, configItr->second);
		}

		std::pair<Optional<TenantMapEntry>, bool> entry =
		    wait(TenantAPI::createTenantTransaction(&ryw->getTransaction(), tenantEntry));

		return entry.second;
	}

	ACTOR static Future<Void> createTenants(
	    ReadYourWritesTransaction* ryw,
	    std::map<TenantName, std::vector<std::pair<Standalone<StringRef>, Optional<Value>>>> tenants) {
		state Future<int64_t> tenantCountFuture =
		    TenantMetadata::tenantCount().getD(&ryw->getTransaction(), Snapshot::False, 0);
		state int64_t nextId;
		wait(store(nextId, TenantAPI::getNextTenantId(&ryw->getTransaction())));

		state std::vector<Future<bool>> createFutures;
		for (auto const& [tenant, config] : tenants) {
			createFutures.push_back(createTenant(ryw, tenant, config, nextId++));
		}
		TenantMetadata::lastTenantId().set(&ryw->getTransaction(), nextId - 1);

		wait(waitForAll(createFutures));

		state int numCreatedTenants = 0;
		for (auto f : createFutures) {
			if (f.get()) {
				++numCreatedTenants;
			}
		}

		// Check the tenant count here rather than rely on the createTenantTransaction check because we don't have RYW
		int64_t tenantCount = wait(tenantCountFuture);
		fmt::print("Special keys create tenant: {} {} {}\n", tenants.size(), tenantCount, numCreatedTenants);
		for (auto t : tenants) {
			fmt::print("  {}\n", printable(t.first));
		}
		if (tenantCount + numCreatedTenants > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			throw cluster_no_capacity();
		}

		return Void();
	}

	ACTOR static Future<Void> changeTenantConfig(
	    ReadYourWritesTransaction* ryw,
	    TenantName tenantName,
	    std::vector<std::pair<Standalone<StringRef>, Optional<Value>>> configEntries) {
		state TenantMapEntry originalEntry = wait(TenantAPI::getTenantTransaction(&ryw->getTransaction(), tenantName));
		state TenantMapEntry updatedEntry = originalEntry;

		state std::vector<std::pair<Standalone<StringRef>, Optional<Value>>>::iterator configItr;
		for (configItr = configEntries.begin(); configItr != configEntries.end(); ++configItr) {
			if (configItr->first == "tenant_group"_sr) {
				if (configItr->second.present()) {
					Optional<int64_t> groupId = wait(
					    TenantMetadata::tenantGroupNameIndex().get(&ryw->getTransaction(), configItr->second.get()));
					if (!groupId.present()) {
						throw tenant_group_not_found();
					}
					updatedEntry.tenantGroup = groupId;
				}
			}
			updatedEntry.configure(configItr->first, configItr->second);
		}

		wait(TenantAPI::configureTenantTransaction(&ryw->getTransaction(), originalEntry, updatedEntry));
		return Void();
	}

	ACTOR static Future<Void> deleteSingleTenant(ReadYourWritesTransaction* ryw, TenantName tenantName) {
		state Optional<TenantMapEntry> tenantEntry =
		    wait(TenantAPI::tryGetTenantTransaction(&ryw->getTransaction(), tenantName));
		if (tenantEntry.present()) {
			fmt::print("Delete tenant {} {} with group {}\n",
			           printable(tenantEntry.get().tenantName),
			           tenantEntry.get().id,
			           tenantEntry.get().tenantGroup.orDefault(-1));
			wait(TenantAPI::deleteTenantTransaction(&ryw->getTransaction(), tenantEntry.get().id));
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenantRange(ReadYourWritesTransaction* ryw,
	                                            TenantName beginTenant,
	                                            TenantName endTenant) {
		fmt::print("Delete tenant range: {} {}\n", printable(beginTenant), printable(endTenant));
		state std::vector<std::pair<TenantName, int64_t>> tenants = wait(
		    TenantAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, CLIENT_KNOBS->TOO_MANY));

		if (tenants.size() == CLIENT_KNOBS->TOO_MANY) {
			TraceEvent(SevWarn, "DeleteTenantRangeTooLange")
			    .detail("BeginTenant", beginTenant)
			    .detail("EndTenant", endTenant);
			ryw->setSpecialKeySpaceErrorMsg(
			    ManagementAPIError::toJsonString(false, "delete tenants", "too many tenants to range delete"));
			throw special_keys_api_failure();
		}

		std::vector<Future<Void>> deleteFutures;
		for (auto tenant : tenants) {
			deleteFutures.push_back(deleteSingleTenant(ryw, tenant.first));
		}

		wait(waitForAll(deleteFutures));
		return Void();
	}

public:
	const inline static KeyRangeRef submoduleRange = KeyRangeRef("tenant/"_sr, "tenant0"_sr);
	const inline static KeyRangeRef mapSubRange = KeyRangeRef("map/"_sr, "map0"_sr);
	const inline static KeyRangeRef configureSubRange = KeyRangeRef("configure/"_sr, "configure0"_sr);
	const inline static KeyRangeRef renameSubRange = KeyRangeRef("rename/"_sr, "rename0"_sr);

	explicit TenantRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override {
		return getTenantRange(ryw, kr, limitsHint);
	}

	ACTOR static Future<Optional<std::string>> commitImpl(TenantRangeImpl* self, ReadYourWritesTransaction* ryw) {
		state std::vector<Future<Void>> tenantManagementFutures;

		state KeyRangeMap<std::pair<bool, Optional<Value>>>::Ranges ranges =
		    ryw->getSpecialKeySpaceWriteMap().containedRanges(self->range);

		state std::vector<std::pair<KeyRangeRef, Optional<Value>>> mapMutations;
		state std::map<TenantName, std::vector<std::pair<Standalone<StringRef>, Optional<Value>>>> configMutations;
		state std::set<TenantName> renameSet;
		state std::vector<std::pair<TenantName, TenantName>> renameMutations;

		tenantManagementFutures.push_back(TenantAPI::checkTenantMode(&ryw->getTransaction(), ClusterType::STANDALONE));

		for (auto range : ranges) {
			if (!range.value().first) {
				continue;
			}

			state KeyRangeRef adjustedRange =
			    range.range()
			        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
			        .removePrefix(submoduleRange.begin);

			if (mapSubRange.intersects(adjustedRange)) {
				adjustedRange = mapSubRange & adjustedRange;
				adjustedRange = removePrefix(adjustedRange, mapSubRange.begin, "\xff"_sr);
				mapMutations.push_back(std::make_pair(adjustedRange, range.value().second));
			} else if (configureSubRange.intersects(adjustedRange) && adjustedRange.singleKeyRange()) {
				StringRef configTupleStr = adjustedRange.begin.removePrefix(configureSubRange.begin);
				try {
					Tuple tuple = Tuple::unpack(configTupleStr);
					if (tuple.size() != 2) {
						throw invalid_tuple_index();
					}
					configMutations[tuple.getString(0)].push_back(
					    std::make_pair(tuple.getString(1), range.value().second));
				} catch (Error& e) {
					TraceEvent(SevWarn, "InvalidTenantConfigurationKey").error(e).detail("Key", adjustedRange.begin);
					ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
					    false, "configure tenant", "invalid tenant configuration key"));
					throw special_keys_api_failure();
				}
			} else if (renameSubRange.intersects(adjustedRange)) {
				StringRef oldName = adjustedRange.begin.removePrefix(renameSubRange.begin);
				StringRef newName = range.value().second.get();
				// Do not allow overlapping renames in the same commit
				// e.g. A->B + B->C, D->D
				if (renameSet.count(oldName) || renameSet.count(newName) || oldName == newName) {
					ryw->setSpecialKeySpaceErrorMsg(
					    ManagementAPIError::toJsonString(false, "rename tenant", "tenant rename conflict"));
					throw special_keys_api_failure();
				}
				renameSet.insert(oldName);
				renameSet.insert(newName);
				renameMutations.push_back(std::make_pair(oldName, newName));
			}
		}

		std::map<TenantName, std::vector<std::pair<Standalone<StringRef>, Optional<Value>>>> tenantsToCreate;
		for (auto mapMutation : mapMutations) {
			TenantNameRef tenantName = mapMutation.first.begin;
			auto set_iter = renameSet.lower_bound(tenantName);
			if (set_iter != renameSet.end() && mapMutation.first.contains(*set_iter)) {
				ryw->setSpecialKeySpaceErrorMsg(
				    ManagementAPIError::toJsonString(false, "rename tenant", "tenant rename conflict"));
				throw special_keys_api_failure();
			}
			if (mapMutation.second.present()) {
				std::vector<std::pair<Standalone<StringRef>, Optional<Value>>> createMutations;
				auto itr = configMutations.find(tenantName);
				if (itr != configMutations.end()) {
					createMutations = itr->second;
					configMutations.erase(itr);
				}
				tenantsToCreate[tenantName] = createMutations;
			} else {
				// For a single key clear, just issue the delete
				if (mapMutation.first.singleKeyRange()) {
					tenantManagementFutures.push_back(deleteSingleTenant(ryw, tenantName));

					// Configuration changes made to a deleted tenant are discarded
					configMutations.erase(tenantName);
				} else {
					tenantManagementFutures.push_back(deleteTenantRange(ryw, tenantName, mapMutation.first.end));

					// Configuration changes made to a deleted tenant are discarded
					configMutations.erase(configMutations.lower_bound(tenantName),
					                      configMutations.lower_bound(mapMutation.first.end));
				}
			}
		}

		if (!tenantsToCreate.empty()) {
			tenantManagementFutures.push_back(createTenants(ryw, tenantsToCreate));
		}
		for (auto configMutation : configMutations) {
			if (renameSet.count(configMutation.first)) {
				ryw->setSpecialKeySpaceErrorMsg(
				    ManagementAPIError::toJsonString(false, "rename tenant", "tenant rename conflict"));
				throw special_keys_api_failure();
			}
			tenantManagementFutures.push_back(changeTenantConfig(ryw, configMutation.first, configMutation.second));
		}

		for (auto renameMutation : renameMutations) {
			tenantManagementFutures.push_back(TenantAPI::renameTenantTransaction(
			    &ryw->getTransaction(), renameMutation.first, renameMutation.second));
		}

		wait(waitForAll(tenantManagementFutures));
		return Optional<std::string>();
	}

	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override { return commitImpl(this, ryw); }
};

#include "flow/unactorcompiler.h"
#endif