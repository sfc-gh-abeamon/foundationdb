
/*
 * MetaclusterConsistency.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include "fdbclient/FDBOptions.g.h"
#include "flow/BooleanParam.h"
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_G_H
#include "fdbserver/workloads/MetaclusterConsistency.actor.g.h"
#elif !defined(WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H)
#define WORKLOADS_METACLUSTER_CONSISTENCY_ACTOR_H

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbserver/workloads/TenantConsistency.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DECLARE_BOOLEAN_PARAM(AllowPartialMetaclusterOperations);

template <class DB>
class MetaclusterConsistencyCheck {
private:
	Reference<DB> managementDb;
	AllowPartialMetaclusterOperations allowPartialMetaclusterOperations = AllowPartialMetaclusterOperations::True;

	struct ManagementClusterData {
		Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		std::map<ClusterName, DataClusterMetadata> dataClusters;
		KeyBackedRangeResult<Tuple> clusterCapacityTuples;
		KeyBackedRangeResult<std::pair<ClusterName, int64_t>> clusterTenantCounts;
		KeyBackedRangeResult<Tuple> clusterTenantTuples;
		KeyBackedRangeResult<Tuple> clusterTenantGroupTuples;

		std::map<int64_t, TenantMapEntry> tenantMap;
		std::map<int64_t, TenantGroupEntry> tenantGroupMap;

		std::map<ClusterName, std::set<int64_t>> clusterTenantMap;
		std::map<ClusterName, std::set<int64_t>> clusterTenantGroupMap;

		int64_t tenantCount;
		int64_t tenantGroupCount;
		RangeResult systemTenantSubspaceKeys;
	};

	ManagementClusterData managementMetadata;

	// Note: this check can only be run on metaclusters with a reasonable number of tenants, as should be
	// the case with the current metacluster simulation workloads
	static inline const int metaclusterMaxTenants = 10e6;

	ACTOR static Future<Void> loadManagementClusterMetadata(MetaclusterConsistencyCheck* self) {
		state Reference<typename DB::TransactionT> managementTr = self->managementDb->createTransaction();
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantList;
		state KeyBackedRangeResult<std::pair<int64_t, TenantGroupEntry>> tenantGroupList;

		loop {
			try {
				managementTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				state typename transaction_future_type<typename DB::TransactionT, RangeResult>::type
				    systemTenantSubspaceKeysFuture = managementTr->getRange(prefixRange(TenantMetadata::subspace()), 1);

				wait(store(self->managementMetadata.metaclusterRegistration,
				           MetaclusterMetadata::metaclusterRegistration().get(managementTr)) &&
				     store(self->managementMetadata.dataClusters,
				           MetaclusterAPI::listClustersTransaction(
				               managementTr, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1)) &&
				     store(self->managementMetadata.clusterCapacityTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterCapacityIndex.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(self->managementMetadata.clusterTenantCounts,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantCount.getRange(
				               managementTr, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
				     store(self->managementMetadata.clusterTenantTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.clusterTenantGroupTuples,
				           MetaclusterAPI::ManagementClusterMetadata::clusterTenantGroupIndex.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.tenantCount,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantCount.getD(
				               managementTr, Snapshot::False, 0)) &&
				     store(self->managementMetadata.tenantGroupCount,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantGroupCount.getD(
				               managementTr, Snapshot::False, 0)) &&
				     store(tenantList,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantMap.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(tenantGroupList,
				           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantGroupMap.getRange(
				               managementTr, {}, {}, metaclusterMaxTenants)) &&
				     store(self->managementMetadata.systemTenantSubspaceKeys,
				           safeThreadFutureToFuture(systemTenantSubspaceKeysFuture)));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(managementTr->onError(e)));
			}
		}

		self->managementMetadata.tenantMap =
		    std::map<int64_t, TenantMapEntry>(tenantList.results.begin(), tenantList.results.end());

		self->managementMetadata.tenantGroupMap =
		    std::map<int64_t, TenantGroupEntry>(tenantGroupList.results.begin(), tenantGroupList.results.end());

		for (auto t : self->managementMetadata.clusterTenantTuples.results) {
			ASSERT_EQ(t.size(), 3);
			TenantName tenantName = t.getString(1);
			int64_t tenantId = t.getInt(2);
			ASSERT(tenantName == self->managementMetadata.tenantMap[tenantId].tenantName);
			self->managementMetadata.clusterTenantMap[t.getString(0)].insert(tenantId);
		}

		for (auto t : self->managementMetadata.clusterTenantGroupTuples.results) {
			ASSERT_EQ(t.size(), 3);
			TenantGroupName tenantGroupName = t.getString(1);
			int64_t tenantGroupId = t.getInt(2);
			ASSERT(tenantGroupName == self->managementMetadata.tenantGroupMap[tenantGroupId].name);
			self->managementMetadata.clusterTenantGroupMap[t.getString(0)].insert(tenantGroupId);
		}

		return Void();
	}

	void validateManagementCluster() {
		ASSERT(managementMetadata.metaclusterRegistration.present());
		ASSERT_EQ(managementMetadata.metaclusterRegistration.get().clusterType, ClusterType::METACLUSTER_MANAGEMENT);
		ASSERT(managementMetadata.metaclusterRegistration.get().id ==
		           managementMetadata.metaclusterRegistration.get().metaclusterId &&
		       managementMetadata.metaclusterRegistration.get().name ==
		           managementMetadata.metaclusterRegistration.get().metaclusterName);
		ASSERT_LE(managementMetadata.dataClusters.size(), CLIENT_KNOBS->MAX_DATA_CLUSTERS);
		ASSERT_LE(managementMetadata.tenantCount, metaclusterMaxTenants);
		ASSERT_LE(managementMetadata.tenantGroupCount, metaclusterMaxTenants);
		ASSERT(managementMetadata.clusterCapacityTuples.results.size() <= managementMetadata.dataClusters.size() &&
		       !managementMetadata.clusterCapacityTuples.more);
		ASSERT(managementMetadata.clusterTenantCounts.results.size() <= managementMetadata.dataClusters.size() &&
		       !managementMetadata.clusterTenantCounts.more);
		ASSERT(managementMetadata.clusterTenantTuples.results.size() == managementMetadata.tenantCount &&
		       !managementMetadata.clusterTenantTuples.more);
		fmt::print("ClusterTenantGroupTuples: {}, tenant count: {}, more: {}\n",
		           managementMetadata.clusterTenantGroupTuples.results.size(),
		           managementMetadata.tenantCount,
		           managementMetadata.clusterTenantGroupTuples.more);
		ASSERT(managementMetadata.clusterTenantGroupTuples.results.size() == managementMetadata.tenantGroupCount &&
		       !managementMetadata.clusterTenantGroupTuples.more);
		ASSERT_EQ(managementMetadata.tenantMap.size(), managementMetadata.tenantCount);
		ASSERT_LE(managementMetadata.tenantGroupMap.size(), managementMetadata.tenantGroupCount);
		ASSERT_EQ(managementMetadata.clusterTenantGroupTuples.results.size(), managementMetadata.tenantGroupMap.size());

		// Parse the cluster capacity index. Check that no cluster is represented in the index more than once.
		std::map<ClusterName, int64_t> clusterAllocatedMap;
		for (auto t : managementMetadata.clusterCapacityTuples.results) {
			ASSERT_EQ(t.size(), 2);
			auto result = clusterAllocatedMap.emplace(t.getString(1), t.getInt(0));
			ASSERT(result.second);
		}

		// Validate various properties for each data cluster
		int numFoundInAllocatedMap = 0;
		int numFoundInTenantGroupMap = 0;
		for (auto [clusterName, clusterMetadata] : managementMetadata.dataClusters) {
			// If the cluster has capacity, it should be in the capacity index and have the correct count of
			// allocated tenants stored there
			auto allocatedItr = clusterAllocatedMap.find(clusterName);
			if (!clusterMetadata.entry.hasCapacity()) {
				ASSERT(allocatedItr == clusterAllocatedMap.end());
			} else {
				ASSERT_EQ(allocatedItr->second, clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInAllocatedMap;
			}

			// Check that the number of tenant groups in the cluster is smaller than the allocated number of tenant
			// groups.
			auto tenantGroupItr = managementMetadata.clusterTenantGroupMap.find(clusterName);
			if (tenantGroupItr != managementMetadata.clusterTenantGroupMap.end()) {
				ASSERT_LE(tenantGroupItr->second.size(), clusterMetadata.entry.allocated.numTenantGroups);
				++numFoundInTenantGroupMap;
			}
		}
		// Check that we exhausted the cluster capacity index and the cluster tenant group index
		ASSERT_EQ(numFoundInAllocatedMap, clusterAllocatedMap.size());
		ASSERT_EQ(numFoundInTenantGroupMap, managementMetadata.clusterTenantGroupMap.size());

		// Check that our cluster tenant counters match the number of tenants in the cluster index
		std::map<ClusterName, int64_t> countsMap(managementMetadata.clusterTenantCounts.results.begin(),
		                                         managementMetadata.clusterTenantCounts.results.end());
		for (auto [cluster, clusterTenants] : managementMetadata.clusterTenantMap) {
			auto itr = countsMap.find(cluster);
			ASSERT((clusterTenants.empty() && itr == countsMap.end()) || itr->second == clusterTenants.size());
		}

		// Iterate through all tenants and verify related metadata
		std::map<ClusterName, int> clusterAllocated;
		std::set<int64_t> processedTenantGroups;
		for (auto [tenantId, entry] : managementMetadata.tenantMap) {
			ASSERT(entry.assignedCluster.present());

			// Each tenant should be assigned to the same cluster where it is stored in the cluster tenant index
			auto clusterItr = managementMetadata.clusterTenantMap.find(entry.assignedCluster.get());
			ASSERT(clusterItr != managementMetadata.clusterTenantMap.end());
			ASSERT(clusterItr->second.count(tenantId));

			if (entry.tenantGroup.present()) {
				// The tenant group should be stored in the same cluster where it is stored in the cluster tenant
				// group index
				auto clusterTenantGroupItr = managementMetadata.clusterTenantGroupMap.find(entry.assignedCluster.get());
				ASSERT(clusterTenantGroupItr != managementMetadata.clusterTenantGroupMap.end());
				ASSERT(clusterTenantGroupItr->second.count(entry.tenantGroup.get()));
			} else {
				// Track the actual tenant group allocation per cluster (a tenant with no group counts against the
				// allocation)
				++clusterAllocated[entry.assignedCluster.get()];
			}
		}

		// Each tenant group in the tenant group map should be present in the cluster tenant group map
		// and have the correct cluster assigned to it.
		for (auto [id, entry] : managementMetadata.tenantGroupMap) {
			++clusterAllocated[entry.assignedCluster.get()];
			ASSERT(entry.assignedCluster.present());
			auto clusterItr = managementMetadata.clusterTenantGroupMap.find(entry.assignedCluster.get());
			ASSERT(clusterItr->second.count(id));
		}

		// The actual allocation for each cluster should match what is stored in the cluster metadata
		for (auto [name, allocated] : clusterAllocated) {
			auto itr = managementMetadata.dataClusters.find(name);
			ASSERT(itr != managementMetadata.dataClusters.end());
			ASSERT_EQ(allocated, itr->second.entry.allocated.numTenantGroups);
		}

		// We should not be storing any data in the `\xff` tenant subspace.
		ASSERT(managementMetadata.systemTenantSubspaceKeys.empty());
	}

	ACTOR static Future<Void> validateDataCluster(MetaclusterConsistencyCheck* self,
	                                              ClusterName clusterName,
	                                              DataClusterMetadata clusterMetadata) {
		state Reference<IDatabase> dataDb = wait(MetaclusterAPI::openDatabase(clusterMetadata.connectionString));
		state Reference<ITransaction> dataTr = dataDb->createTransaction();

		state Optional<MetaclusterRegistrationEntry> dataClusterRegistration;
		state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> dataClusterTenantList;
		state KeyBackedRangeResult<std::pair<int64_t, TenantGroupEntry>> dataClusterTenantGroupList;

		state TenantConsistencyCheck<IDatabase> tenantConsistencyCheck(dataDb);
		wait(tenantConsistencyCheck.run());

		loop {
			try {
				dataTr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(dataClusterRegistration, MetaclusterMetadata::metaclusterRegistration().get(dataTr)) &&
				     store(dataClusterTenantList,
				           TenantMetadata::tenantMap().getRange(
				               dataTr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)) &&
				     store(dataClusterTenantGroupList,
				           TenantMetadata::tenantGroupMap().getRange(
				               dataTr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));

				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(dataTr->onError(e)));
			}
		}

		state std::map<int64_t, TenantMapEntry> dataClusterTenantMap(dataClusterTenantList.results.begin(),
		                                                             dataClusterTenantList.results.end());
		state std::map<int64_t, TenantGroupEntry> dataClusterTenantGroupMap(dataClusterTenantGroupList.results.begin(),
		                                                                    dataClusterTenantGroupList.results.end());

		ASSERT(dataClusterRegistration.present());
		ASSERT_EQ(dataClusterRegistration.get().clusterType, ClusterType::METACLUSTER_DATA);
		ASSERT(dataClusterRegistration.get().matches(self->managementMetadata.metaclusterRegistration.get()));
		ASSERT(dataClusterRegistration.get().name == clusterName);
		ASSERT(dataClusterRegistration.get().id == clusterMetadata.entry.id);

		auto& expectedTenants = self->managementMetadata.clusterTenantMap[clusterName];

		for (auto t : expectedTenants) {
			fmt::print("Expected tenant: {}\n", t);
		}
		for (auto t : dataClusterTenantMap) {
			fmt::print("Data cluster tenant: {} {} {}\n",
			           t.first,
			           printable(t.second.tenantName),
			           t.second.tenantGroup.orDefault(-1));
		}

		std::map<int64_t, int> groupExpectedTenantCounts;
		if (!self->allowPartialMetaclusterOperations) {
			ASSERT_EQ(dataClusterTenantMap.size(), expectedTenants.size());
		} else {
			ASSERT_LE(dataClusterTenantMap.size(), expectedTenants.size());
			for (auto tenantName : expectedTenants) {
				TenantMapEntry const& metaclusterEntry = self->managementMetadata.tenantMap[tenantName];
				if (!dataClusterTenantMap.count(tenantName)) {
					if (metaclusterEntry.tenantGroup.present()) {
						groupExpectedTenantCounts.try_emplace(metaclusterEntry.tenantGroup.get(), 0);
					}
					ASSERT(metaclusterEntry.tenantState == TenantState::REGISTERING ||
					       metaclusterEntry.tenantState == TenantState::REMOVING);
				} else if (metaclusterEntry.tenantGroup.present()) {
					++groupExpectedTenantCounts[metaclusterEntry.tenantGroup.get()];
				}
			}
		}

		for (auto [tenantId, entry] : dataClusterTenantMap) {
			ASSERT(expectedTenants.count(tenantId));
			TenantMapEntry const& metaclusterEntry = self->managementMetadata.tenantMap[tenantId];
			ASSERT(!entry.assignedCluster.present());
			ASSERT_EQ(entry.id, metaclusterEntry.id);
			ASSERT(entry.tenantName == metaclusterEntry.tenantName);

			ASSERT_EQ(entry.tenantState, TenantState::READY);
			if (!self->allowPartialMetaclusterOperations) {
				ASSERT_EQ(metaclusterEntry.tenantState, TenantState::READY);
			}
			if (metaclusterEntry.tenantState != TenantState::UPDATING_CONFIGURATION &&
			    metaclusterEntry.tenantState != TenantState::REMOVING) {
				ASSERT_EQ(entry.configurationSequenceNum, metaclusterEntry.configurationSequenceNum);
			} else {
				ASSERT_LE(entry.configurationSequenceNum, metaclusterEntry.configurationSequenceNum);
			}

			if (entry.configurationSequenceNum == metaclusterEntry.configurationSequenceNum) {
				ASSERT(entry.tenantGroup == metaclusterEntry.tenantGroup);
			}
		}

		auto& expectedTenantGroups = self->managementMetadata.clusterTenantGroupMap[clusterName];
		if (!self->allowPartialMetaclusterOperations) {
			ASSERT_EQ(dataClusterTenantGroupMap.size(), expectedTenantGroups.size());
		} else {
			ASSERT_LE(dataClusterTenantGroupMap.size(), expectedTenantGroups.size());
			for (auto const& name : expectedTenantGroups) {
				if (!dataClusterTenantGroupMap.count(name)) {
					auto itr = groupExpectedTenantCounts.find(name);
					ASSERT(itr != groupExpectedTenantCounts.end());
					ASSERT_EQ(itr->second, 0);
				}
			}
		}
		for (auto const& [name, entry] : dataClusterTenantGroupMap) {
			ASSERT(expectedTenantGroups.count(name));
			ASSERT(!entry.assignedCluster.present());
		}

		return Void();
	}

	ACTOR static Future<Void> run(MetaclusterConsistencyCheck* self) {
		state TenantConsistencyCheck<DB> managementTenantConsistencyCheck(self->managementDb);
		wait(managementTenantConsistencyCheck.run());
		wait(loadManagementClusterMetadata(self));
		self->validateManagementCluster();

		state std::vector<Future<Void>> dataClusterChecks;
		state std::map<ClusterName, DataClusterMetadata>::iterator dataClusterItr;
		for (auto [clusterName, clusterMetadata] : self->managementMetadata.dataClusters) {
			dataClusterChecks.push_back(validateDataCluster(self, clusterName, clusterMetadata));
		}
		wait(waitForAll(dataClusterChecks));

		return Void();
	}

public:
	MetaclusterConsistencyCheck() {}
	MetaclusterConsistencyCheck(Reference<DB> managementDb,
	                            AllowPartialMetaclusterOperations allowPartialMetaclusterOperations)
	  : managementDb(managementDb), allowPartialMetaclusterOperations(allowPartialMetaclusterOperations) {}

	Future<Void> run() { return run(this); }
};

#include "flow/unactorcompiler.h"

#endif
