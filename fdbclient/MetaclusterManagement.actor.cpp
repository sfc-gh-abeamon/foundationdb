/*
 * MetaclusterManagement.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/actorcompiler.h" // has to be last include

namespace MetaclusterAPI {

std::pair<ClusterUsage, ClusterUsage> metaclusterCapacity(std::map<ClusterName, DataClusterMetadata> const& clusters) {
	ClusterUsage tenantGroupCapacity;
	ClusterUsage tenantGroupsAllocated;
	for (auto cluster : clusters) {
		tenantGroupCapacity.numTenantGroups +=
		    std::max(cluster.second.entry.capacity.numTenantGroups, cluster.second.entry.allocated.numTenantGroups);
		tenantGroupsAllocated.numTenantGroups += cluster.second.entry.allocated.numTenantGroups;
	}
	return { tenantGroupCapacity, tenantGroupsAllocated };
}

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString) {
	if (g_network->isSimulated()) {
		Reference<IClusterConnectionRecord> clusterFile =
		    makeReference<ClusterConnectionMemoryRecord>(connectionString);
		Database nativeDb = Database::createDatabase(clusterFile, -1);
		Reference<IDatabase> threadSafeDb =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(nativeDb)));
		return MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeDb);
	} else {
		return MultiVersionApi::api->createDatabaseFromConnectionString(connectionString.toString().c_str());
	}
}

ACTOR Future<ClusterName> checkClusterAvailability(Reference<IDatabase> dataClusterDb, ClusterName clusterName) {
	state Reference<ITransaction> tr = dataClusterDb->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->addWriteConflictRange(
			    KeyRangeRef("\xff/metacluster/availability_check"_sr, "\xff/metacluster/availability_check\x00"_sr));
			wait(safeThreadFutureToFuture(tr->commit()));
			return clusterName;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<ClusterName> assignCluster(Reference<ITransaction> tr, Optional<ClusterName> desiredCluster) {
	state std::vector<Future<Reference<IDatabase>>> dataClusterDbs;
	state std::vector<ClusterName> dataClusterNames;
	state std::vector<Future<ClusterName>> clusterAvailabilityChecks;
	// Get a set of the most full clusters that still have capacity
	// If preferred cluster is specified, look for that one.
	if (desiredCluster.present()) {
		DataClusterMetadata dataClusterMetadata = wait(getClusterTransaction(tr, desiredCluster.get()));
		if (!dataClusterMetadata.entry.hasCapacity()) {
			throw cluster_no_capacity();
		}
		dataClusterNames.push_back(desiredCluster.get());
	} else {
		state KeyBackedSet<Tuple>::RangeResultType availableClusters =
		    wait(ManagementClusterMetadata::clusterCapacityIndex.getRange(
		        tr, {}, {}, CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_CLUSTERS_TO_CHECK, Snapshot::False, Reverse::True));
		if (availableClusters.results.empty()) {
			throw metacluster_no_capacity();
		}
		for (auto clusterTuple : availableClusters.results) {
			dataClusterNames.push_back(clusterTuple.getString(1));
		}
	}
	for (auto dataClusterName : dataClusterNames) {
		dataClusterDbs.push_back(getAndOpenDatabase(tr, dataClusterName));
	}
	wait(waitForAll(dataClusterDbs));
	// Check the availability of our set of clusters
	for (int i = 0; i < dataClusterDbs.size(); ++i) {
		clusterAvailabilityChecks.push_back(checkClusterAvailability(dataClusterDbs[i].get(), dataClusterNames[i]));
	}

	// Wait for a successful availability check from some cluster. We prefer the most full cluster, but if it
	// doesn't return quickly we may choose another.
	Optional<Void> clusterAvailabilityCheck = wait(timeout(
	    success(clusterAvailabilityChecks[0]) ||
	        (delay(CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_FIRST_CHOICE_DELAY) && waitForAny(clusterAvailabilityChecks)),
	    CLIENT_KNOBS->METACLUSTER_ASSIGNMENT_AVAILABILITY_TIMEOUT));

	if (!clusterAvailabilityCheck.present()) {
		// If no clusters were available for long enough, then we throw an error and try again
		throw transaction_too_old();
	}

	// Get the first cluster that was available
	state Optional<ClusterName> chosenCluster;
	for (auto f : clusterAvailabilityChecks) {
		if (f.isReady()) {
			chosenCluster = f.get();
			break;
		}
	}

	ASSERT(chosenCluster.present());
	return chosenCluster.get();
}

KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())>&
ManagementClusterMetadata::dataClusters() {
	static KeyBackedObjectMap<ClusterName, DataClusterEntry, decltype(IncludeVersion())> instance(
	    "metacluster/dataCluster/metadata/"_sr, IncludeVersion());
	return instance;
}

KeyBackedMap<ClusterName,
             ClusterConnectionString,
             TupleCodec<ClusterName>,
             ManagementClusterMetadata::ConnectionStringCodec>
    ManagementClusterMetadata::dataClusterConnectionRecords("metacluster/dataCluster/connectionString/"_sr);

KeyBackedSet<Tuple> ManagementClusterMetadata::clusterCapacityIndex("metacluster/clusterCapacityIndex/"_sr);
KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>>
    ManagementClusterMetadata::clusterTenantCount("metacluster/clusterTenantCount/"_sr);
KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantIndex("metacluster/dataCluster/tenantMap/"_sr);
KeyBackedSet<Tuple> ManagementClusterMetadata::clusterTenantGroupIndex("metacluster/dataCluster/tenantGroupMap/"_sr);
KeyBackedMap<ClusterName, int64_t, TupleCodec<ClusterName>, BinaryCodec<int64_t>>
    ManagementClusterMetadata::clusterTenantGroupCount("metacluster/clusterTenantGroupCount/"_sr);

TenantMetadataSpecification& ManagementClusterMetadata::tenantMetadata() {
	static TenantMetadataSpecification instance(""_sr);
	return instance;
}

}; // namespace MetaclusterAPI