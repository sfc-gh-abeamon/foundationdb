/*
 * TenantManagementWorkload.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/MetaclusterConsistency.actor.h"
#include "fdbserver/workloads/TenantConsistency.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/ApiVersion.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"
#include "libb64/decode.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantManagementWorkload : TestWorkload {
	static constexpr auto NAME = "TenantManagement";

	struct TenantData {
		Reference<Tenant> tenant;
		Optional<TenantGroupName> tenantGroup;
		bool empty;

		TenantData() : empty(true) {}
		TenantData(int64_t id, Optional<TenantGroupName> tenantGroup, bool empty)
		  : tenant(makeReference<Tenant>(id)), tenantGroup(tenantGroup), empty(empty) {}
		TenantData(int64_t id, Optional<TenantName> tName, Optional<TenantGroupName> tenantGroup, bool empty)
		  : tenant(makeReference<Tenant>(id, tName)), tenantGroup(tenantGroup), empty(empty) {}
	};

	struct TenantGroupData {
		int64_t id = -1;
		TenantGroupData() = default;
		TenantGroupData(int64_t id) : id(id) {}
	};

	std::map<TenantName, TenantData> createdTenants;
	std::map<TenantGroupName, TenantGroupData> createdTenantGroups;
	// Contains references to ALL tenants that were created by this client
	// Possible to have been deleted, but will be tracked historically here
	std::vector<Reference<Tenant>> allTestTenants;
	int64_t maxId = -1;

	const Key keyName = "key"_sr;
	const Key testParametersKey = nonMetadataSystemKeys.begin.withSuffix("/tenant_test/test_parameters"_sr);
	const Value noTenantValue = "no_tenant"_sr;
	const TenantName tenantNamePrefix = "tenant_management_workload_"_sr;
	const ClusterName dataClusterName = "cluster1"_sr;
	TenantName localTenantNamePrefix;
	TenantName localTenantGroupNamePrefix;

	const Key specialKeysTenantMapPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                           .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                           .withSuffix(TenantRangeImpl::mapSubRange.begin);
	const Key specialKeysTenantConfigPrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl::configureSubRange.begin);
	const Key specialKeysTenantRenamePrefix = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT)
	                                              .begin.withSuffix(TenantRangeImpl::submoduleRange.begin)
	                                              .withSuffix(TenantRangeImpl::renameSubRange.begin);

	int maxTenants;
	int maxTenantGroups;
	double testDuration;
	bool useMetacluster;
	bool singleClient;

	Version oldestDeletionVersion = 0;
	Version newestDeletionVersion = 0;

	Reference<IDatabase> mvDb;
	Database dataDb;
	bool hasNoTenantKey = false; // whether this workload has non-tenant key

	// This test exercises multiple different ways to work with tenants
	enum class OperationType {
		// Use the special key-space APIs
		SPECIAL_KEYS,
		// Use the ManagementAPI functions that take a Database object and implement a retry loop
		MANAGEMENT_DATABASE,
		// Use the ManagementAPI functions that take a Transaction object
		MANAGEMENT_TRANSACTION,
		// Use the Metacluster API, if applicable. Note: not all APIs have a metacluster variant,
		// and if there isn't one this will choose one of the other options.
		METACLUSTER
	};

	OperationType randomOperationType() {
		double metaclusterProb = useMetacluster ? 0.9 : 0.1;

		if (deterministicRandom()->random01() < metaclusterProb) {
			return OperationType::METACLUSTER;
		} else {
			return (OperationType)deterministicRandom()->randomInt(0, 3);
		}
	}

	TenantManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		singleClient = getOption(options, "singleClient"_sr, false);

		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);
		localTenantGroupNamePrefix = format("%stenantgroup_%d_", tenantNamePrefix.toString().c_str(), clientId);

		bool defaultUseMetacluster = false;
		if (clientId == 0 && g_network->isSimulated() && !g_simulator->extraDatabases.empty()) {
			defaultUseMetacluster = deterministicRandom()->coinflip();
		}

		useMetacluster = getOption(options, "useMetacluster"_sr, defaultUseMetacluster);
	}

	struct TestParameters {
		constexpr static FileIdentifier file_identifier = 1527576;

		bool useMetacluster = false;

		TestParameters() {}
		TestParameters(bool useMetacluster) : useMetacluster(useMetacluster) {}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, useMetacluster);
		}

		Value encode() const { return ObjectWriter::toValue(*this, Unversioned()); }
		static TestParameters decode(ValueRef const& value) {
			return ObjectReader::fromStringRef<TestParameters>(value, Unversioned());
		}
	};

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0 && g_network->isSimulated() && BUGGIFY) {
			IKnobCollection::getMutableGlobalKnobCollection().setKnob(
			    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
		}

		if (clientId == 0 || !singleClient) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	Future<Optional<Key>> waitDataDbTenantModeChange() const {
		return runRYWTransaction(dataDb, [](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return tr->get("\xff"_sr); // just a meaningless read
		});
	}

	// Set a key outside of all tenants to make sure that our tenants aren't writing to the regular key-space
	Future<Void> writeNonTenantKey() const {
		return runRYWTransaction(dataDb, [this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			tr->set(keyName, noTenantValue);
			return Future<Void>(Void());
		});
	}

	// load test parameters from meta-cluster
	ACTOR static Future<Void> loadTestParameters(Database cx, TenantManagementWorkload* self) {
		state Transaction tr(cx);
		// Read the parameters chosen and saved by client 0
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Optional<Value> val = wait(tr.get(self->testParametersKey));
				if (val.present()) {
					TestParameters params = TestParameters::decode(val.get());
					self->useMetacluster = params.useMetacluster;
					return Void();
				}
				wait(delay(1.0));
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// send test parameters from metacluster
	ACTOR static Future<Void> sendTestParameters(Database cx, TenantManagementWorkload* self) {
		// Communicates test parameters to all other clients by storing it in a key
		fmt::print("Sending test parameters ...\n");
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				tr.set(self->testParametersKey, TestParameters(self->useMetacluster).encode());
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	// only the first client will do this setup
	ACTOR static Future<Void> firstClientSetup(Database cx, TenantManagementWorkload* self) {
		if (self->useMetacluster) {
			fmt::print("Create metacluster and register data cluster ... \n");
			// Configure the metacluster (this changes the tenant mode)
			wait(success(MetaclusterAPI::createMetacluster(cx.getReference(), "management_cluster"_sr)));

			DataClusterEntry entry;
			entry.capacity.numTenantGroups = 1e9;
			wait(MetaclusterAPI::registerCluster(
			    self->mvDb, self->dataClusterName, g_simulator->extraDatabases[0], entry));

			ASSERT(g_simulator->extraDatabases.size() == 1);
			self->dataDb = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0], cx->defaultTenant);
			// wait for tenant mode change on dataDB
			wait(success(self->waitDataDbTenantModeChange()));
		} else {
			self->dataDb = cx;
		}
		wait(sendTestParameters(cx, self));

		if (self->dataDb->getTenantMode() != TenantMode::REQUIRED) {
			self->hasNoTenantKey = true;
			wait(self->writeNonTenantKey());
		}
		return Void();
	}

	ACTOR static Future<Void> _setup(Database cx, TenantManagementWorkload* self) {
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));

		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		self->mvDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

		if (self->clientId == 0) {
			wait(firstClientSetup(cx, self));
		} else {
			wait(loadTestParameters(cx, self));
			if (self->useMetacluster) {
				ASSERT(g_simulator->extraDatabases.size() == 1);
				self->dataDb =
				    Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0], cx->defaultTenant);
			} else {
				self->dataDb = cx;
			}
		}

		return Void();
	}

	ACTOR template <class DB>
	static Future<Versionstamp> getLastTenantModification(Reference<DB> db, OperationType type) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				if (type == OperationType::METACLUSTER) {
					Versionstamp vs =
					    wait(MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().lastTenantModification.getD(
					        tr, Snapshot::False, Versionstamp()));
					return vs;
				}
				Versionstamp vs =
				    wait(TenantMetadata::lastTenantModification().getD(tr, Snapshot::False, Versionstamp()));
				return vs;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	ACTOR template <class DB>
	static Future<Version> getLatestReadVersion(Reference<DB> db, OperationType type) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();
		loop {
			try {
				Version readVersion = wait(safeThreadFutureToFuture(tr->getReadVersion()));
				return readVersion;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	static Future<Versionstamp> getLastTenantModification(TenantManagementWorkload* self, OperationType type) {
		if (type == OperationType::METACLUSTER) {
			return getLastTenantModification(self->mvDb, type);
		} else {
			return getLastTenantModification(self->dataDb.getReference(), type);
		}
	}

	static Future<Version> getLatestReadVersion(TenantManagementWorkload* self, OperationType type) {
		if (type == OperationType::METACLUSTER) {
			return getLatestReadVersion(self->mvDb, type);
		} else {
			return getLatestReadVersion(self->dataDb.getReference(), type);
		}
	}

	TenantName chooseTenantName(bool allowSystemTenant) {
		TenantName tenant(format(
		    "%s%08d", localTenantNamePrefix.toString().c_str(), deterministicRandom()->randomInt(0, maxTenants)));
		if (allowSystemTenant && deterministicRandom()->random01() < 0.02) {
			tenant = tenant.withPrefix("\xff"_sr);
		}

		return tenant;
	}

	Optional<TenantGroupName> chooseTenantGroup(bool allowSystemTenantGroup, bool allowEmptyGroup = true) {
		Optional<TenantGroupName> tenantGroup;
		if (!allowEmptyGroup || deterministicRandom()->coinflip()) {
			tenantGroup = TenantGroupNameRef(format("%s%08d",
			                                        localTenantGroupNamePrefix.toString().c_str(),
			                                        deterministicRandom()->randomInt(0, maxTenantGroups)));
			if (allowSystemTenantGroup && deterministicRandom()->random01() < 0.02) {
				tenantGroup = tenantGroup.get().withPrefix("\xff"_sr);
			}
		}

		return tenantGroup;
	}

	Future<Optional<TenantMapEntry>> tryGetTenant(TenantName tenantName, OperationType operationType) {
		if (operationType == OperationType::METACLUSTER) {
			return MetaclusterAPI::tryGetTenant(mvDb, tenantName);
		} else {
			return TenantAPI::tryGetTenant(dataDb.getReference(), tenantName);
		}
	}

	Future<Optional<TenantGroupEntry>> tryGetTenantGroup(TenantGroupName tenantGroupName, OperationType operationType) {
		if (operationType == OperationType::METACLUSTER) {
			return MetaclusterAPI::tryGetTenantGroup(mvDb, tenantGroupName);
		} else {
			return TenantAPI::tryGetTenantGroup(dataDb.getReference(), tenantGroupName);
		}
	}

	// Creates tenant(s) using the specified operation type
	ACTOR static Future<Void> createTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                           std::map<TenantName, Optional<TenantGroupName>> tenantsToCreate,
	                                           OperationType operationType,
	                                           TenantManagementWorkload* self) {
		state TenantMapEntry tenantEntry;
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto [tenantName, groupName] : tenantsToCreate) {
				tr->set(self->specialKeysTenantMapPrefix.withSuffix(tenantName), ""_sr);
				if (groupName.present()) {
					tr->set(self->specialKeysTenantConfigPrefix.withSuffix(
					            Tuple::makeTuple(tenantName, "tenant_group"_sr).pack()),
					        groupName.get());
				}
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantsToCreate.size() == 1);
			wait(success(TenantAPI::createTenant(self->dataDb.getReference(),
			                                     tenantsToCreate.begin()->first,
			                                     tenantEntry,
			                                     tenantsToCreate.begin()->second)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state int64_t nextId = wait(TenantAPI::getNextTenantId(tr));
			state std::vector<Future<Void>> createFutures;

			state std::map<TenantName, Optional<TenantGroupName>>::iterator tenantItr;
			for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
				tenantEntry = TenantMapEntry(nextId++, tenantItr->first, TenantState::READY);
				if (tenantItr->second.present()) {
					Optional<int64_t> groupId =
					    wait(TenantMetadata::tenantGroupNameIndex().get(tr, tenantItr->second.get()));
					if (!groupId.present()) {
						throw tenant_group_not_found();
					}
					tenantEntry.tenantGroup = groupId;
				}
				createFutures.push_back(success(TenantAPI::createTenantTransaction(tr, tenantEntry)));
			}
			TenantMetadata::lastTenantId().set(tr, nextId - 1);
			wait(waitForAll(createFutures));
			wait(tr->commit());
		} else {
			ASSERT(tenantsToCreate.size() == 1);
			tenantEntry.tenantName = tenantsToCreate.begin()->first;
			AssignClusterAutomatically assignClusterAutomatically = AssignClusterAutomatically::True;
			if (deterministicRandom()->coinflip()) {
				tenantEntry.assignedCluster = self->dataClusterName;
				assignClusterAutomatically = AssignClusterAutomatically::False;
			}
			wait(MetaclusterAPI::createTenant(
			    self->mvDb, tenantEntry, tenantsToCreate.begin()->second, assignClusterAutomatically));
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();
		int numTenants = 1;

		// For transaction-based operations, test creating multiple tenants in the same transaction
		if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numTenants = deterministicRandom()->randomInt(1, 5);
		}

		// Tracks whether any tenant exists in the database or not. This variable is updated if we have to retry
		// the creation.
		state bool alreadyExists = false;

		// True if all of the groups being created in exist
		state bool groupsExist = true;

		// True if any tenant name starts with \xff
		state bool hasSystemTenant = false;

		state int newTenants = 0;
		state std::map<TenantName, Optional<TenantGroupName>> tenantsToCreate;
		for (int i = 0; i < numTenants; ++i) {
			TenantName tenant = self->chooseTenantName(true);
			while (tenantsToCreate.count(tenant)) {
				tenant = self->chooseTenantName(true);
			}

			if (self->createdTenants.count(tenant)) {
				alreadyExists = true;
			} else {
				++newTenants;
			}

			Optional<TenantGroupName> tenantGroup = self->chooseTenantGroup(true);
			tenantsToCreate[tenant] = tenantGroup;

			hasSystemTenant = hasSystemTenant || tenant.startsWith("\xff"_sr);
			groupsExist = groupsExist && (!tenantGroup.present() || self->createdTenantGroups.count(tenantGroup.get()));
		}

		// If any tenant existed at the start of this function, then we expect the creation to fail or be a no-op,
		// depending on the type of create operation being executed
		state bool existedAtStart = alreadyExists;

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);
		state int64_t minTenantCount = std::numeric_limits<int64_t>::max();
		state int64_t finalTenantCount = 0;

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		// First, attempt to create the tenants
		state bool retried = false;

		fmt::print("Creating tenants: {} {}\n", existedAtStart, tenantsToCreate.size());
		for (auto t : tenantsToCreate) {
			fmt::print("  {}\n", printable(t.first));
		}

		loop {
			try {
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					wait(store(finalTenantCount, TenantMetadata::tenantCount().getD(tr, Snapshot::False, 0)));
					minTenantCount = std::min(finalTenantCount, minTenantCount);
				}

				Optional<Void> result = wait(timeout(createTenantImpl(tr, tenantsToCreate, operationType, self),
				                                     deterministicRandom()->randomInt(1, 30)));

				if (result.present()) {
					// If any tenant group doesn't exist, then the creation should fail
					ASSERT(groupsExist);

					// Make sure that we had capacity to create the tenants. This cannot be validated for
					// database operations because we cannot determine the tenant count in the same transaction
					// that the tenant is created
					if (operationType == OperationType::SPECIAL_KEYS ||
					    operationType == OperationType::MANAGEMENT_TRANSACTION) {
						fmt::print("Checking capacity: {} {} {} {} {}\n",
						           minTenantCount,
						           newTenants,
						           CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER,
						           (int)operationType,
						           printable(tenantsToCreate.begin()->first));
						ASSERT(minTenantCount + newTenants <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
					} else {
						// Database operations shouldn't get here if the tenant already exists
						ASSERT(!alreadyExists);
					}

					break;
				}

				retried = true;
				tr->reset();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_already_exists && !existedAtStart) {
					// If we retried the creation after our initial attempt succeeded, then we proceed with the
					// rest of the creation steps normally.
					ASSERT(operationType == OperationType::METACLUSTER ||
					       operationType == OperationType::MANAGEMENT_DATABASE);
					ASSERT(retried);
					break;
				} else if (e.code() == error_code_invalid_tenant_name) {
					ASSERT(hasSystemTenant);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// Confirm that we overshot our capacity. This check cannot be done for database operations
					// because we cannot transactionally get the tenant count with the creation.
					if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
					    operationType == OperationType::SPECIAL_KEYS) {
						ASSERT(finalTenantCount + newTenants > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
					}
					return Void();
				} else if (e.code() == error_code_tenant_group_not_found) {
					ASSERT(!groupsExist);
					return Void();
				}

				// Database-based operations should not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_already_exists) {
						ASSERT(existedAtStart);
					} else {
						ASSERT(tenantsToCreate.size() == 1);
						TraceEvent(SevError, "CreateTenantFailure")
						    .error(e)
						    .detail("TenantName", tenantsToCreate.begin()->first);
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						for (auto [tenant, _] : tenantsToCreate) {
							TraceEvent(SevError, "CreateTenantFailure").error(e).detail("TenantName", tenant);
						}
						return Void();
					}
				}
			}

			// Check the state of the first created tenant
			Optional<TenantMapEntry> resultEntry =
			    wait(self->tryGetTenant(tenantsToCreate.begin()->first, operationType));

			if (resultEntry.present()) {
				if (resultEntry.get().tenantState == TenantState::READY) {
					// The tenant now exists, so we will retry and expect the creation to react accordingly
					alreadyExists = true;
				} else {
					// Only a metacluster tenant creation can end up in a partially created state
					// We should be able to retry and pick up where we left off
					ASSERT(operationType == OperationType::METACLUSTER);
					ASSERT(resultEntry.get().tenantState == TenantState::REGISTERING);
				}
			}
		}

		// Check that using the wrong creation type fails depending on whether we are using a metacluster
		ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));

		// Database-based creation modes will fail if the tenant already existed
		if (operationType == OperationType::MANAGEMENT_DATABASE || operationType == OperationType::METACLUSTER) {
			ASSERT(!existedAtStart);
		}

		// It is not legal to create a tenant starting with \xff
		ASSERT(!hasSystemTenant);

		state std::map<TenantName, Optional<TenantGroupName>>::iterator tenantItr;
		for (tenantItr = tenantsToCreate.begin(); tenantItr != tenantsToCreate.end(); ++tenantItr) {
			// Ignore any tenants that already existed
			if (self->createdTenants.count(tenantItr->first)) {
				fmt::print(
				    "Tenant already created: {} {}\n", printable(tenantItr->first), printable(tenantItr->second));
				continue;
			}

			// Read the created tenant object and verify that its state is correct
			state Optional<TenantMapEntry> entry = wait(self->tryGetTenant(tenantItr->first, operationType));
			state Optional<TenantGroupEntry> groupEntry;
			if (tenantItr->second.present()) {
				wait(store(groupEntry, self->tryGetTenantGroup(tenantItr->second.get(), operationType)));
			}

			fmt::print("Creating tenant group1 {} {} {} {}\n",
			           printable(tenantItr->first),
			           entry.get().tenantGroup.orDefault(-1),
			           groupEntry.map(&TenantGroupEntry::id).orDefault(-1),
			           printable(tenantItr->second));

			ASSERT(entry.present());
			ASSERT_GT(entry.get().id, self->maxId);
			ASSERT(entry.get().tenantGroup == groupEntry.map(&TenantGroupEntry::id));
			ASSERT_EQ(entry.get().tenantState, TenantState::READY);

			Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
			ASSERT_GT(currentVersionstamp.version, originalReadVersion);

			if (self->useMetacluster) {
				// In a metacluster, we should also see that the tenant was created on the data cluster
				Optional<TenantMapEntry> dataEntry =
				    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), tenantItr->first));
				ASSERT(dataEntry.present());
				ASSERT_EQ(dataEntry.get().id, entry.get().id);
				ASSERT(dataEntry.get().tenantGroup == entry.get().tenantGroup);
				ASSERT_EQ(dataEntry.get().tenantState, TenantState::READY);
			}

			// Update our local tenant state to include the newly created one
			self->maxId = entry.get().id;
			fmt::print("Adding to created tenants: {} {}\n", entry.get().id, printable(tenantItr->first));
			TenantData tData = TenantData(entry.get().id, tenantItr->first, tenantItr->second, true);
			self->createdTenants[tenantItr->first] = tData;
			self->allTestTenants.push_back(tData.tenant);

			// If this tenant has a tenant group, create or update the entry for it
			if (tenantItr->second.present()) {
				fmt::print("Creating tenant group2 {} {} {}\n",
				           printable(tenantItr->first),
				           entry.get().tenantGroup.get(),
				           printable(tenantItr->second.get()));
				auto itr = self->createdTenantGroups.find(tenantItr->second.get());
				ASSERT(itr != self->createdTenantGroups.end());
				ASSERT(itr->second.id == entry.get().tenantGroup);
			}

			// Randomly decide to insert a key into the tenant
			state bool insertData = deterministicRandom()->random01() < 0.5;
			if (insertData) {
				state Transaction insertTr(self->dataDb, self->createdTenants[tenantItr->first].tenant);
				loop {
					try {
						// The value stored in the key will be the name of the tenant
						insertTr.set(self->keyName, tenantItr->first);
						wait(insertTr.commit());
						break;
					} catch (Error& e) {
						wait(insertTr.onError(e));
					}
				}

				self->createdTenants[tenantItr->first].empty = false;

				// Make sure that the key inserted correctly concatenates the tenant prefix with the
				// relative key
				state Transaction checkTr(self->dataDb);
				loop {
					try {
						checkTr.setOption(FDBTransactionOptions::RAW_ACCESS);
						Optional<Value> val = wait(checkTr.get(self->keyName.withPrefix(entry.get().prefix)));
						ASSERT(val.present());
						ASSERT(val.get() == tenantItr->first);
						break;
					} catch (Error& e) {
						wait(checkTr.onError(e));
					}
				}
			}

			// Perform some final tenant validation
			wait(checkTenantContents(self, tenantItr->first, self->createdTenants[tenantItr->first]));
		}

		return Void();
	}

	// set a random watch on a tenant
	ACTOR static Future<Void> watchTenant(TenantManagementWorkload* self, Reference<Tenant> tenant) {
		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->dataDb, tenant));
			try {
				state Future<Void> watch = tr->watch(doubleToTestKey(deterministicRandom()->random01()));
				wait(tr->commit());
				wait(watch);
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	void checkWatchOnDeletedTenant(std::vector<std::pair<TenantName, Future<ErrorOr<Void>>>>& watchFutures) {
		for (auto& [t, f] : watchFutures) {
			auto& res = f.get();
			if (res.isError(error_code_tenant_removed) || res.isError(error_code_tenant_not_found)) {
				CODE_PROBE(res.isError(error_code_tenant_removed),
				           "Watch Triggered because the tenant is deleted during watch");
			} else {
				TraceEvent(SevError, "WatchDeletedTenantCheckFailed")
				    .detail("Tenant", t)
				    .detail("IsError", res.isError() ? res.getError().what() : "Void()");
			}
		}
	}

	ACTOR static Future<Void> clearTenantData(TenantManagementWorkload* self, TenantName tenantName) {
		state Transaction clearTr(self->dataDb, self->createdTenants[tenantName].tenant);
		loop {
			try {
				clearTr.clear(self->keyName);
				wait(clearTr.commit());
				auto itr = self->createdTenants.find(tenantName);
				ASSERT(itr != self->createdTenants.end());
				itr->second.empty = true;
				return Void();
			} catch (Error& e) {
				wait(clearTr.onError(e));
			}
		}
	}

	// Deletes the tenant or tenant range using the specified operation type
	ACTOR static Future<Void> deleteTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                           TenantName beginTenant,
	                                           Optional<TenantName> endTenant,
	                                           std::map<TenantName, int64_t> tenants,
	                                           OperationType operationType,
	                                           TenantManagementWorkload* self) {
		fmt::print("Delete tenant impl {} {}\n", printable(beginTenant), (int)operationType);
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			Key key = self->specialKeysTenantMapPrefix.withSuffix(beginTenant);
			if (endTenant.present()) {
				tr->clear(KeyRangeRef(key, self->specialKeysTenantMapPrefix.withSuffix(endTenant.get())));
			} else {
				tr->clear(key);
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(!endTenant.present() && tenants.size() == 1);
			wait(TenantAPI::deleteTenant(self->dataDb.getReference(), beginTenant));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> deleteFutures;
			for (auto const& [name, id] : tenants) {
				if (id != TenantInfo::INVALID_TENANT) {
					deleteFutures.push_back(TenantAPI::deleteTenantTransaction(tr, id));
				}
			}

			wait(waitForAll(deleteFutures));
			wait(tr->commit());
		} else { // operationType == OperationType::METACLUSTER
			ASSERT(!endTenant.present() && tenants.size() == 1);
			// Read the entry first and then issue delete by ID
			// getTenant throwing tenant_not_found will break some test cases because it is not wrapped
			// by runManagementTransaction. For such cases, fall back to delete by name and allow
			// the errors to flow through there
			Optional<TenantMapEntry> entry = wait(MetaclusterAPI::tryGetTenant(self->mvDb, beginTenant));
			if (entry.present() && deterministicRandom()->coinflip()) {
				wait(MetaclusterAPI::deleteTenant(self->mvDb, entry.get().id));
				CODE_PROBE(true, "Deleted tenant by ID");
			} else {
				wait(MetaclusterAPI::deleteTenant(self->mvDb, beginTenant));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> deleteTenant(TenantManagementWorkload* self) {
		state bool watchTenantCheck = deterministicRandom()->coinflip();
		state TenantName beginTenant = self->chooseTenantName(true);
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// For transaction-based deletion, we randomly allow the deletion of a range of tenants
		state Optional<TenantName> endTenant =
		    operationType != OperationType::MANAGEMENT_DATABASE && operationType != OperationType::METACLUSTER &&
		            !beginTenant.startsWith("\xff"_sr) && deterministicRandom()->random01() < 0.2
		        ? Optional<TenantName>(self->chooseTenantName(false))
		        : Optional<TenantName>();

		if (endTenant.present() && endTenant < beginTenant) {
			TenantName temp = beginTenant;
			beginTenant = endTenant.get();
			endTenant = temp;
		}

		auto itr = self->createdTenants.find(beginTenant);

		// True if the beginTenant should exist and be deletable. This is updated if a deletion fails and gets
		// retried.
		state bool alreadyExists = itr != self->createdTenants.end();

		// True if the beginTenant existed at the start of this function
		state bool existedAtStart = alreadyExists;

		// True if all of the tenants in the range are empty and can be deleted
		state bool isEmpty = true;

		// True if we expect that some tenant will be deleted
		state bool anyExists = alreadyExists;

		// Collect a list of all tenants that we expect should be deleted by this operation
		state std::map<TenantName, int64_t> tenants;
		if (!endTenant.present()) {
			tenants[beginTenant] = anyExists ? itr->second.tenant->id() : TenantInfo::INVALID_TENANT;
		} else if (endTenant.present()) {
			for (auto itr = self->createdTenants.lower_bound(beginTenant);
			     itr != self->createdTenants.end() && itr->first < endTenant.get();
			     ++itr) {
				tenants[itr->first] = itr->second.tenant->id();
				anyExists = true;
			}
		}

		// Check whether each tenant is empty.
		state std::map<TenantName, int64_t>::iterator tenantItr;
		state std::vector<std::pair<TenantName, Future<ErrorOr<Void>>>> watchFutures;
		try {
			if (alreadyExists || endTenant.present()) {
				for (tenantItr = tenants.begin(); tenantItr != tenants.end(); ++tenantItr) {
					// For most tenants, we will delete the contents and make them empty
					if (deterministicRandom()->random01() < 0.9) {
						wait(clearTenantData(self, tenantItr->first));

						// watch the tenant to be deleted
						if (watchTenantCheck) {
							watchFutures.emplace_back(
							    tenantItr->first,
							    errorOr(watchTenant(self, self->createdTenants[tenantItr->first].tenant)));
						}
					}
					// Otherwise, we will just report the current emptiness of the tenant
					else {
						auto itr = self->createdTenants.find(tenantItr->first);
						ASSERT(itr != self->createdTenants.end());
						isEmpty = isEmpty && itr->second.empty;
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "DeleteTenantFailure")
			    .error(e)
			    .detail("TenantName", beginTenant)
			    .detail("EndTenant", endTenant);
			return Void();
		}

		// Tenants deletion retry loop
		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				// Attempt to delete the tenant(s)
				state bool retried = false;
				loop {
					try {
						state Version beforeVersion =
						    wait(getLatestReadVersion(self, OperationType::MANAGEMENT_DATABASE));
						Optional<Void> result =
						    wait(timeout(deleteTenantImpl(tr, beginTenant, endTenant, tenants, operationType, self),
						                 deterministicRandom()->randomInt(1, 30)));

						if (result.present()) {
							if (anyExists) {
								if (self->oldestDeletionVersion == 0 && !tenants.empty()) {
									Version afterVersion =
									    wait(self->getLatestReadVersion(self, OperationType::MANAGEMENT_DATABASE));
									self->oldestDeletionVersion = afterVersion;
								}
								self->newestDeletionVersion = beforeVersion;
							}

							// Database operations shouldn't get here if the tenant didn't exist
							ASSERT(operationType == OperationType::SPECIAL_KEYS ||
							       operationType == OperationType::MANAGEMENT_TRANSACTION || alreadyExists);
							break;
						}

						retried = true;
						tr->reset();
					} catch (Error& e) {
						// If we retried the deletion after our initial attempt succeeded, then we proceed with the
						// rest of the deletion steps normally. Otherwise, the deletion happened elsewhere and we
						// failed here, so we can rethrow the error.
						if (e.code() == error_code_tenant_not_found && existedAtStart) {
							ASSERT(operationType == OperationType::METACLUSTER ||
							       operationType == OperationType::MANAGEMENT_DATABASE);
							ASSERT(retried);
							break;
						} else if (e.code() == error_code_grv_proxy_memory_limit_exceeded ||
						           e.code() == error_code_batch_transaction_throttled) {
							// GRV proxy returns an error
							wait(tr->onError(e));
							continue;
						} else {
							throw;
						}
					}

					if (!tenants.empty()) {
						// Check the state of the first deleted tenant
						Optional<TenantMapEntry> resultEntry =
						    wait(self->tryGetTenant(tenants.begin()->first, operationType));

						if (!resultEntry.present()) {
							alreadyExists = false;
						} else if (resultEntry.get().tenantState == TenantState::REMOVING) {
							ASSERT(operationType == OperationType::METACLUSTER);
						} else {
							ASSERT(resultEntry.get().tenantState == TenantState::READY);
						}
					}
				}

				// The management transaction operation is a no-op if there are no tenants to delete
				if (!anyExists && operationType == OperationType::MANAGEMENT_TRANSACTION) {
					return Void();
				}

				// The special keys operation is a no-op if the begin and end tenant are equal (i.e. the range is
				// empty)
				if (endTenant.present() && beginTenant == endTenant.get() &&
				    operationType == OperationType::SPECIAL_KEYS) {
					return Void();
				}

				// Check that using the wrong deletion type fails depending on whether we are using a metacluster
				ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));

				// Transaction-based operations do not fail if the tenant isn't present. If we attempted to delete a
				// single tenant that didn't exist, we can just return.
				if (!existedAtStart && !endTenant.present() &&
				    (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				     operationType == OperationType::SPECIAL_KEYS)) {
					return Void();
				}

				ASSERT(existedAtStart || endTenant.present());

				// Deletion should not succeed if any tenant in the range wasn't empty
				ASSERT(isEmpty);

				if (tenants.size() > 0) {
					Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
					ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				}

				// Update our local state to remove the deleted tenants
				for (auto const& [name, id] : tenants) {
					auto itr = self->createdTenants.find(name);
					ASSERT(itr != self->createdTenants.end());
					fmt::print("Removing from created tenants: {} {}\n", id, printable(name));
					self->createdTenants.erase(name);
				}

				// check for watch result
				state int wi = 0;
				for (; wi < watchFutures.size(); ++wi) {
					wait(ready(watchFutures[wi].second));
				}
				self->checkWatchOnDeletedTenant(watchFutures);

				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_empty) {
					ASSERT(!isEmpty);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				}

				// Database-based operations do not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_not_found) {
						ASSERT(!existedAtStart && !endTenant.present());
					} else {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "DeleteTenantFailure")
						    .error(e)
						    .detail("TenantName", beginTenant)
						    .detail("EndTenant", endTenant);
						return Void();
					}
				}
			}
		}
	}

	// Performs some validation on a tenant's contents
	ACTOR static Future<Void> checkTenantContents(TenantManagementWorkload* self,
	                                              TenantName tenantName,
	                                              TenantData tenantData) {
		state Transaction tr(self->dataDb, self->createdTenants[tenantName].tenant);
		loop {
			try {
				// We only every store a single key in each tenant. Therefore we expect a range read of the entire
				// tenant to return either 0 or 1 keys, depending on whether that key has been set.
				state RangeResult result = wait(tr.getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));

				// An empty tenant should have no data
				if (tenantData.empty) {
					ASSERT(result.size() == 0);
				}
				// A non-empty tenant should have our single key. The value of that key should be the name of the
				// tenant.
				else {
					ASSERT(result.size() == 1);
					ASSERT(result[0].key == self->keyName);
					ASSERT(result[0].value == tenantName);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	// Convert the JSON document returned by the special-key space when reading tenant metadata
	// into a TenantMapEntry
	static TenantMapEntry jsonToTenantMapEntry(ValueRef tenantJson) {
		json_spirit::mValue jsonObject;
		json_spirit::read_string(tenantJson.toString(), jsonObject);
		JSONDoc jsonDoc(jsonObject);

		int64_t id;

		std::string name;
		std::string base64Name;
		std::string printableName;
		std::string prefix;
		std::string base64Prefix;
		std::string printablePrefix;
		std::string tenantStateStr;
		std::string base64TenantGroup;
		std::string printableTenantGroup;
		std::string assignedClusterStr;

		jsonDoc.get("id", id);
		jsonDoc.get("name.base64", base64Name);
		jsonDoc.get("name.printable", printableName);

		name = base64::decoder::from_string(base64Name);
		ASSERT(name == unprintable(printableName));

		jsonDoc.get("prefix.base64", base64Prefix);
		jsonDoc.get("prefix.printable", printablePrefix);

		prefix = base64::decoder::from_string(base64Prefix);
		ASSERT(prefix == unprintable(printablePrefix));

		jsonDoc.get("tenant_state", tenantStateStr);

		Optional<int64_t> tenantGroup;
		if (jsonDoc.has("tenant_group")) {
			int64_t tenantGroupId;
			jsonDoc.get("tenant_group", tenantGroupId);
			tenantGroup = tenantGroupId;
		}

		Optional<ClusterName> assignedCluster;
		if (jsonDoc.tryGet("assigned_cluster", assignedClusterStr)) {
			assignedCluster = ClusterNameRef(assignedClusterStr);
		}

		TenantMapEntry entry(id, TenantNameRef(name), TenantMapEntry::stringToTenantState(tenantStateStr), tenantGroup);
		ASSERT(entry.prefix == prefix);
		return entry;
	}

	// Gets the metadata for a tenant using the specified operation type
	ACTOR static Future<TenantMapEntry> getTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                                  TenantName tenant,
	                                                  OperationType operationType,
	                                                  TenantManagementWorkload* self) {
		state TenantMapEntry entry;
		if (operationType == OperationType::SPECIAL_KEYS) {
			Key key = self->specialKeysTenantMapPrefix.withSuffix(tenant);
			Optional<Value> value = wait(tr->get(key));
			if (!value.present()) {
				throw tenant_not_found();
			}
			entry = TenantManagementWorkload::jsonToTenantMapEntry(value.get());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(entry, TenantAPI::getTenant(self->dataDb.getReference(), tenant)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(entry, TenantAPI::getTenantTransaction(tr, tenant)));
		} else {
			wait(store(entry, MetaclusterAPI::getTenant(self->mvDb, tenant)));
		}

		return entry;
	}

	ACTOR static Future<Void> getTenant(TenantManagementWorkload* self) {
		state TenantName tenant = self->chooseTenantName(true);
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// True if the tenant should should exist and return a result
		auto itr = self->createdTenants.find(tenant);
		state bool alreadyExists = itr != self->createdTenants.end() &&
		                           !(operationType == OperationType::METACLUSTER && !self->useMetacluster);
		state TenantData tenantData = alreadyExists ? itr->second : TenantData();

		loop {
			try {
				// Get the tenant metadata and check that it matches our local state
				state TenantMapEntry entry = wait(getTenantImpl(tr, tenant, operationType, self));
				ASSERT(alreadyExists);
				ASSERT(entry.id == tenantData.tenant->id());
				ASSERT(entry.tenantGroup.present() == tenantData.tenantGroup.present());
				if (tenantData.tenantGroup.present()) {
					ASSERT(entry.tenantGroup.get() == self->createdTenantGroups[tenantData.tenantGroup.get()].id);
				}
				wait(checkTenantContents(self, tenant, tenantData));
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!alreadyExists);
					return Void();
				}

				// Transaction-based operations should retry
				else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				         operationType == OperationType::SPECIAL_KEYS) {
					try {
						retry = true;
						wait(tr->onError(e));
						retry = true;
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantFailure").error(error).detail("TenantName", tenant);
					return Void();
				}
			}
		}
	}

	// Gets a list of tenants using the specified operation type
	ACTOR static Future<std::vector<std::pair<TenantName, TenantMapEntry>>> listTenantsImpl(
	    Reference<ReadYourWritesTransaction> tr,
	    TenantName beginTenant,
	    TenantName endTenant,
	    int limit,
	    OperationType operationType,
	    TenantManagementWorkload* self) {
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;

		if (operationType == OperationType::SPECIAL_KEYS) {
			KeyRange range = KeyRangeRef(beginTenant, endTenant).withPrefix(self->specialKeysTenantMapPrefix);
			RangeResult results = wait(tr->getRange(range, limit));
			for (auto result : results) {
				tenants.push_back(std::make_pair(result.key.removePrefix(self->specialKeysTenantMapPrefix),
				                                 TenantManagementWorkload::jsonToTenantMapEntry(result.value)));
			}
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(tenants,
			           TenantAPI::listTenantMetadata(self->dataDb.getReference(), beginTenant, endTenant, limit)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(tenants, TenantAPI::listTenantMetadataTransaction(tr, beginTenant, endTenant, limit)));
		} else {
			wait(store(tenants, MetaclusterAPI::listTenantMetadata(self->mvDb, beginTenant, endTenant, limit)));
		}

		return tenants;
	}

	ACTOR static Future<Void> listTenants(TenantManagementWorkload* self) {
		state TenantName beginTenant = self->chooseTenantName(false);
		state TenantName endTenant = self->chooseTenantName(false);
		state int limit = std::min(CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1,
		                           deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		if (beginTenant > endTenant) {
			std::swap(beginTenant, endTenant);
		}

		loop {
			try {
				// Attempt to read the chosen list of tenants
				state std::vector<std::pair<TenantName, TenantMapEntry>> tenants =
				    wait(listTenantsImpl(tr, beginTenant, endTenant, limit, operationType, self));

				// Attempting to read the list of tenants using the metacluster API in a non-metacluster should
				// return nothing in this test
				if (operationType == OperationType::METACLUSTER && !self->useMetacluster) {
					ASSERT(tenants.size() == 0);
					return Void();
				}

				ASSERT(tenants.size() <= limit);

				// Compare the resulting tenant list to the list we expected to get
				auto localItr = self->createdTenants.lower_bound(beginTenant);
				auto tenantMapItr = tenants.begin();
				for (; tenantMapItr != tenants.end(); ++tenantMapItr, ++localItr) {
					ASSERT(localItr != self->createdTenants.end());
					ASSERT(localItr->first == tenantMapItr->first);
				}

				// Make sure the list terminated at the right spot
				ASSERT(tenants.size() == limit || localItr == self->createdTenants.end() ||
				       localItr->first >= endTenant);
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations need to be retried
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
					try {
						retry = true;
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "ListTenantFailure")
					    .error(error)
					    .detail("BeginTenant", beginTenant)
					    .detail("EndTenant", endTenant);

					return Void();
				}
			}
		}
	}

	// Helper function that checks tenant keyspace and updates internal Tenant Map after a rename operation
	ACTOR static Future<Void> verifyTenantRename(TenantManagementWorkload* self,
	                                             TenantName oldTenantName,
	                                             TenantName newTenantName) {
		state Optional<TenantMapEntry> oldTenantEntry =
		    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), oldTenantName));
		state Optional<TenantMapEntry> newTenantEntry =
		    wait(TenantAPI::tryGetTenant(self->dataDb.getReference(), newTenantName));
		ASSERT(!oldTenantEntry.present());
		ASSERT(newTenantEntry.present());
		TenantData tData = self->createdTenants[oldTenantName];
		tData.tenant->name = newTenantName;
		self->createdTenants[newTenantName] = tData;
		fmt::print("Removing from created tenants (rename): {} {} {}\n",
		           tData.tenant->id(),
		           printable(oldTenantName),
		           printable(newTenantName));
		self->createdTenants.erase(oldTenantName);
		state Transaction insertTr(self->dataDb, tData.tenant);
		if (!tData.empty) {
			loop {
				try {
					insertTr.set(self->keyName, newTenantName);
					wait(insertTr.commit());
					break;
				} catch (Error& e) {
					wait(insertTr.onError(e));
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> verifyTenantRenames(TenantManagementWorkload* self,
	                                              std::map<TenantName, TenantName> tenantRenames) {
		state std::map<TenantName, TenantName> tenantRenamesCopy = tenantRenames;
		state std::map<TenantName, TenantName>::iterator iter = tenantRenamesCopy.begin();
		for (; iter != tenantRenamesCopy.end(); ++iter) {
			wait(verifyTenantRename(self, iter->first, iter->second));
			wait(checkTenantContents(self, iter->second, self->createdTenants[iter->second]));
		}
		return Void();
	}

	ACTOR static Future<Void> renameTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                           OperationType operationType,
	                                           std::map<TenantName, TenantName> tenantRenames,
	                                           bool tenantNotFound,
	                                           bool tenantExists,
	                                           bool tenantOverlap,
	                                           TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto& iter : tenantRenames) {
				tr->set(self->specialKeysTenantRenamePrefix.withSuffix(iter.first), iter.second);
			}
			wait(tr->commit());
		} else if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(tenantRenames.size() == 1);
			auto iter = tenantRenames.begin();
			wait(TenantAPI::renameTenant(self->dataDb.getReference(), iter->first, iter->second));
			ASSERT(!tenantNotFound && !tenantExists);
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			std::vector<Future<Void>> renameFutures;
			for (auto& iter : tenantRenames) {
				renameFutures.push_back(success(TenantAPI::renameTenantTransaction(tr, iter.first, iter.second)));
			}
			wait(waitForAll(renameFutures));
			wait(tr->commit());
			ASSERT(!tenantNotFound && !tenantExists);
		} else { // operationType == OperationType::METACLUSTER
			ASSERT(tenantRenames.size() == 1);
			auto iter = tenantRenames.begin();
			wait(MetaclusterAPI::renameTenant(self->mvDb, iter->first, iter->second));
		}
		return Void();
	}

	ACTOR static Future<Void> renameTenant(TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();
		state int numTenants = 1;
		state Reference<ReadYourWritesTransaction> tr = self->dataDb->createTransaction();

		if (operationType == OperationType::SPECIAL_KEYS || operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numTenants = deterministicRandom()->randomInt(1, 5);
		}

		state std::map<TenantName, TenantName> tenantRenames;
		state std::set<TenantName> allTenantNames;

		// Tenant Error flags
		state bool tenantExists = false;
		state bool tenantNotFound = false;
		state bool tenantOverlap = false;
		state bool unknownResult = false;

		for (int i = 0; i < numTenants; ++i) {
			TenantName oldTenant = self->chooseTenantName(false);
			TenantName newTenant = self->chooseTenantName(false);
			bool checkOverlap =
			    oldTenant == newTenant || allTenantNames.count(oldTenant) || allTenantNames.count(newTenant);
			// renameTenantTransaction does not handle rename collisions:
			// reject the rename here if it has overlap and we are doing a transaction operation
			// and then pick another combination
			if (checkOverlap && operationType == OperationType::MANAGEMENT_TRANSACTION) {
				--i;
				continue;
			}
			tenantOverlap = tenantOverlap || checkOverlap;
			tenantRenames[oldTenant] = newTenant;
			allTenantNames.insert(oldTenant);
			allTenantNames.insert(newTenant);
			if (!self->createdTenants.count(oldTenant)) {
				tenantNotFound = true;
			}
			if (self->createdTenants.count(newTenant)) {
				tenantExists = true;
			}
		}

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				wait(renameTenantImpl(
				    tr, operationType, tenantRenames, tenantNotFound, tenantExists, tenantOverlap, self));
				wait(verifyTenantRenames(self, tenantRenames));
				Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
				ASSERT_GT(currentVersionstamp.version, originalReadVersion);
				// Check that using the wrong rename API fails depending on whether we are using a metacluster
				ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_found) {
					if (unknownResult) {
						wait(verifyTenantRenames(self, tenantRenames));
					} else {
						ASSERT(tenantNotFound);
					}
					return Void();
				} else if (e.code() == error_code_tenant_already_exists) {
					if (unknownResult) {
						wait(verifyTenantRenames(self, tenantRenames));
					} else {
						ASSERT(tenantExists);
					}
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					ASSERT(tenantOverlap);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// This error should only occur on metacluster due to the multi-stage process.
					// Having temporary tenants may exceed capacity, so we disallow the rename.
					ASSERT(operationType == OperationType::METACLUSTER);
					return Void();
				} else {
					try {
						// In the case of commit_unknown_result, assume we continue retrying
						// until it's successful. Next loop around may throw error because it's
						// already been moved, so account for that and update internal map as needed.
						if (e.code() == error_code_commit_unknown_result) {
							ASSERT(operationType != OperationType::MANAGEMENT_DATABASE &&
							       operationType != OperationType::METACLUSTER);
							unknownResult = true;
						}
						wait(tr->onError(e));
					} catch (Error& e) {
						TraceEvent(SevError, "RenameTenantFailure")
						    .error(e)
						    .detail("TenantRenames", describe(tenantRenames));
						return Void();
					}
				}
			}
		}
	}

	// Changes the configuration of a tenant
	ACTOR static Future<Void> configureTenantImpl(Reference<ReadYourWritesTransaction> tr,
	                                              TenantName tenant,
	                                              std::map<Standalone<StringRef>, Optional<Value>> configParameters,
	                                              OperationType operationType,
	                                              bool specialKeysUseInvalidTuple,
	                                              TenantManagementWorkload* self) {
		if (operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (auto const& [config, value] : configParameters) {
				Tuple t;
				if (specialKeysUseInvalidTuple) {
					// Wrong number of items
					if (deterministicRandom()->coinflip()) {
						int numItems = deterministicRandom()->randomInt(0, 3);
						if (numItems > 0) {
							t.append(tenant);
						}
						if (numItems > 1) {
							t.append(config).append(""_sr);
						}
					}
					// Wrong data types
					else {
						if (deterministicRandom()->coinflip()) {
							t.append(0).append(config);
						} else {
							t.append(tenant).append(0);
						}
					}
				} else {
					t.append(tenant).append(config);
				}
				if (value.present()) {
					tr->set(self->specialKeysTenantConfigPrefix.withSuffix(t.pack()), value.get());
				} else {
					tr->clear(self->specialKeysTenantConfigPrefix.withSuffix(t.pack()));
				}
			}

			wait(tr->commit());
			ASSERT(!specialKeysUseInvalidTuple);
		} else if (operationType == OperationType::METACLUSTER) {
			wait(MetaclusterAPI::configureTenant(self->mvDb, tenant, configParameters));
		} else {
			// We don't have a transaction or database variant of this function
			ASSERT(false);
		}

		return Void();
	}

	ACTOR static Future<Void> configureTenant(TenantManagementWorkload* self) {
		state OperationType operationType =
		    deterministicRandom()->coinflip() ? OperationType::SPECIAL_KEYS : OperationType::METACLUSTER;

		state TenantName tenant = self->chooseTenantName(true);
		auto itr = self->createdTenants.find(tenant);
		state bool exists = itr != self->createdTenants.end();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		state std::map<Standalone<StringRef>, Optional<Value>> configuration;
		state Optional<TenantGroupName> newTenantGroup;

		// If true, the options generated may include an unknown option
		state bool hasInvalidOption = deterministicRandom()->random01() < 0.1;

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state bool specialKeysUseInvalidTuple =
		    operationType == OperationType::SPECIAL_KEYS && deterministicRandom()->random01() < 0.1;

		state bool groupExists = true;

		// Generate a tenant group. Sometimes do this at the same time that we include an invalid option to ensure
		// that the configure function still fails
		if (!hasInvalidOption || deterministicRandom()->coinflip()) {
			newTenantGroup = self->chooseTenantGroup(true);
			hasSystemTenantGroup = hasSystemTenantGroup || newTenantGroup.orDefault(""_sr).startsWith("\xff"_sr);
			if (newTenantGroup.present()) {
				groupExists = self->createdTenantGroups.count(newTenantGroup.get());
			}
			configuration["tenant_group"_sr] = newTenantGroup;
		}
		if (hasInvalidOption) {
			configuration["invalid_option"_sr] = ""_sr;
		}

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		loop {
			try {
				wait(configureTenantImpl(tr, tenant, configuration, operationType, specialKeysUseInvalidTuple, self));

				ASSERT(exists);
				ASSERT(!hasInvalidOption);
				ASSERT(!hasSystemTenantGroup);
				ASSERT(!specialKeysUseInvalidTuple);
				Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
				ASSERT_GT(currentVersionstamp.version, originalReadVersion);

				auto itr = self->createdTenants.find(tenant);
				itr->second.tenantGroup = newTenantGroup;
				return Void();
			} catch (Error& e) {
				state Error error = e;
				if (e.code() == error_code_tenant_not_found) {
					ASSERT(!exists);
					return Void();
				} else if (e.code() == error_code_tenant_group_not_found) {
					ASSERT(!groupExists);
					return Void();
				} else if (e.code() == error_code_special_keys_api_failure) {
					ASSERT(hasInvalidOption || specialKeysUseInvalidTuple);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_configuration) {
					ASSERT(hasInvalidOption);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				}

				try {
					wait(tr->onError(e));
				} catch (Error&) {
					TraceEvent(SevError, "ConfigureTenantFailure").error(error).detail("TenantName", tenant);
					return Void();
				}
			}
		}
	}

	// Gets the metadata for a tenant group using the specified operation type
	ACTOR static Future<Optional<TenantGroupEntry>> getTenantGroupImpl(Reference<ReadYourWritesTransaction> tr,
	                                                                   TenantGroupName tenant,
	                                                                   OperationType operationType,
	                                                                   TenantManagementWorkload* self) {
		state Optional<TenantGroupEntry> entry;
		if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(entry, TenantAPI::tryGetTenantGroup(self->dataDb.getReference(), tenant)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
		           operationType == OperationType::SPECIAL_KEYS) {
			// There is no special-keys interface for reading tenant groups currently, so read them
			// using the TenantAPI.
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(entry, TenantAPI::tryGetTenantGroupTransaction(tr, tenant)));
		} else {
			wait(store(entry, MetaclusterAPI::tryGetTenantGroup(self->mvDb, tenant)));
		}

		return entry;
	}

	ACTOR static Future<Void> getTenantGroup(TenantManagementWorkload* self) {
		state TenantGroupName tenantGroup = self->chooseTenantGroup(true, false).get();
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		// True if the tenant group should should exist and return a result
		auto itr = self->createdTenantGroups.find(tenantGroup);
		state bool alreadyExists = itr != self->createdTenantGroups.end() &&
		                           !(operationType == OperationType::METACLUSTER && !self->useMetacluster);

		loop {
			try {
				// Get the tenant group metadata and check that it matches our local state
				state Optional<TenantGroupEntry> entry = wait(getTenantGroupImpl(tr, tenantGroup, operationType, self));
				ASSERT(alreadyExists == entry.present());
				if (entry.present()) {
					ASSERT(entry.get().assignedCluster.present() == (operationType == OperationType::METACLUSTER));
				}
				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations should retry
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
					try {
						wait(tr->onError(e));
						retry = true;
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "GetTenantGroupFailure").error(error).detail("TenantGroupName", tenantGroup);
					return Void();
				}
			}
		}
	}

	// Gets a list of tenant groups using the specified operation type
	ACTOR static Future<std::vector<std::pair<TenantGroupName, int64_t>>> listTenantGroupsImpl(
	    Reference<ReadYourWritesTransaction> tr,
	    TenantGroupName beginTenantGroup,
	    TenantGroupName endTenantGroup,
	    int limit,
	    OperationType operationType,
	    TenantManagementWorkload* self) {
		state std::vector<std::pair<TenantGroupName, int64_t>> tenantGroups;

		if (operationType == OperationType::MANAGEMENT_DATABASE) {
			wait(store(
			    tenantGroups,
			    TenantAPI::listTenantGroups(self->dataDb.getReference(), beginTenantGroup, endTenantGroup, limit)));
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
		           operationType == OperationType::SPECIAL_KEYS) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			wait(store(tenantGroups,
			           TenantAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
		} else {
			wait(store(tenantGroups,
			           MetaclusterAPI::listTenantGroups(self->mvDb, beginTenantGroup, endTenantGroup, limit)));
		}

		return tenantGroups;
	}

	ACTOR static Future<Void> listTenantGroups(TenantManagementWorkload* self) {
		state TenantGroupName beginTenantGroup = self->chooseTenantGroup(false, false).get();
		state TenantGroupName endTenantGroup = self->chooseTenantGroup(false, false).get();
		state int limit = std::min(CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1,
		                           deterministicRandom()->randomInt(1, self->maxTenants * 2));
		state OperationType operationType = self->randomOperationType();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		if (beginTenantGroup > endTenantGroup) {
			std::swap(beginTenantGroup, endTenantGroup);
		}

		loop {
			try {
				// Attempt to read the chosen list of tenant groups
				state std::vector<std::pair<TenantGroupName, int64_t>> tenantGroups =
				    wait(listTenantGroupsImpl(tr, beginTenantGroup, endTenantGroup, limit, operationType, self));

				// Attempting to read the list of tenant groups using the metacluster API in a non-metacluster should
				// return nothing in this test
				if (operationType == OperationType::METACLUSTER && !self->useMetacluster) {
					ASSERT(tenantGroups.size() == 0);
					return Void();
				}

				ASSERT(tenantGroups.size() <= limit);

				// Compare the resulting tenant group list to the list we expected to get
				auto localItr = self->createdTenantGroups.lower_bound(beginTenantGroup);
				auto tenantMapItr = tenantGroups.begin();
				bool failed = false;
				for (; tenantMapItr != tenantGroups.end(); ++tenantMapItr, ++localItr) {
					ASSERT(localItr != self->createdTenantGroups.end());
					fmt::print("Local group: {}, tenant group map: {}\n",
					           printable(localItr->first),
					           printable(tenantMapItr->first));
					if (localItr->first != tenantMapItr->first) {
						failed = true;
						break;
					}
					ASSERT(localItr->first == tenantMapItr->first);
				}
				// Make sure the list terminated at the right spot
				ASSERT(tenantGroups.size() == limit || localItr == self->createdTenantGroups.end() ||
				       localItr->first >= endTenantGroup);
				if (failed) {
					KeyBackedRangeResult<std::pair<int64_t, TenantGroupEntry>> result =
					    wait(TenantMetadata::tenantGroupMap().getRange(tr, {}, {}, 1e6));
					for (auto r : result.results) {
						fmt::print("Tenant group in map: {} {}\n", r.first, printable(r.second.name));
					}
					ASSERT(false);
				}

				return Void();
			} catch (Error& e) {
				state bool retry = false;
				state Error error = e;

				// Transaction-based operations need to be retried
				if (operationType == OperationType::MANAGEMENT_TRANSACTION ||
				    operationType == OperationType::SPECIAL_KEYS) {
					try {
						retry = true;
						wait(tr->onError(e));
					} catch (Error& e) {
						error = e;
						retry = false;
					}
				}

				if (!retry) {
					TraceEvent(SevError, "ListTenantGroupFailure")
					    .error(error)
					    .detail("BeginTenant", beginTenantGroup)
					    .detail("EndTenant", endTenantGroup);

					return Void();
				}
			}
		}
	}

	ACTOR static Future<Void> readTenantKey(TenantManagementWorkload* self) {
		if (self->allTestTenants.size() == 0) {
			return Void();
		}
		state Reference<Tenant> tenant = deterministicRandom()->randomChoice(self->allTestTenants);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb, tenant);
		state TenantName tName = tenant->name.get();
		state bool tenantPresent = false;
		state TenantData tData = TenantData();
		auto itr = self->createdTenants.find(tName);
		if (itr != self->createdTenants.end() && itr->second.tenant->id() == tenant->id()) {
			tenantPresent = true;
			tData = itr->second;
		}
		state bool keyPresent = tenantPresent && !tData.empty;
		loop {
			try {
				Optional<Value> val = wait(tr->get(self->keyName));
				if (val.present()) {
					ASSERT(keyPresent && val.get() == tName);
				} else {
					ASSERT(tenantPresent && tData.empty);
				}
				break;
			} catch (Error& e) {
				state Error err = e;
				if (err.code() == error_code_tenant_not_found) {
					ASSERT(!tenantPresent);
					CODE_PROBE(true, "Attempted to read key from non-existent tenant");
					return Void();
				}
				wait(tr->onError(err));
			}
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0 || !singleClient) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	// Gets a list of tenant groups using the specified operation type
	ACTOR static Future<Void> createTenantGroupImpl(Reference<ReadYourWritesTransaction> tr,
	                                                std::set<TenantGroupName> groupsToCreate,
	                                                OperationType operationType,
	                                                TenantManagementWorkload* self) {
		ASSERT(operationType != OperationType::SPECIAL_KEYS);
		if (operationType == OperationType::MANAGEMENT_DATABASE) {
			ASSERT(groupsToCreate.size() == 1);
			Optional<TenantGroupEntry> entry =
			    wait(TenantAPI::createTenantGroup(self->dataDb.getReference(), *groupsToCreate.begin()));
			ASSERT(entry.present());
		} else if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state int64_t nextId = wait(TenantAPI::getNextTenantId(tr));
			state std::vector<Future<std::pair<Optional<TenantGroupEntry>, bool>>> createFutures;
			for (auto const& name : groupsToCreate) {
				TenantGroupEntry entry(nextId++, name);
				createFutures.push_back(TenantAPI::createTenantGroupTransaction(tr, entry));
			}
			TenantMetadata::lastTenantId().set(tr, nextId - 1);
			wait(waitForAll(createFutures));
		} else {
			ASSERT(groupsToCreate.size() == 1);
			TenantGroupEntry entry;
			entry.name = *groupsToCreate.begin();
			AssignClusterAutomatically assignClusterAutomatically = AssignClusterAutomatically::True;
			if (deterministicRandom()->coinflip()) {
				assignClusterAutomatically = AssignClusterAutomatically::False;
				entry.assignedCluster = self->dataClusterName;
			}
			wait(MetaclusterAPI::createTenantGroup(self->mvDb, entry, assignClusterAutomatically));
		}

		return Void();
	}

	ACTOR static Future<Void> createTenantGroup(TenantManagementWorkload* self) {
		state OperationType operationType = self->randomOperationType();

		// There is no special key interface to create tenant groups
		if (operationType == OperationType::SPECIAL_KEYS) {
			operationType = OperationType::MANAGEMENT_TRANSACTION;
		}

		int numGroups = 1;

		// For transaction-based operations, test creating multiple tenant groups in the same transaction
		if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
			numGroups = deterministicRandom()->randomInt(1, 3);
		}

		// Tracks whether any tenant group exists in the database or not. This variable is updated if we have to retry
		// the creation.
		state bool alreadyExists = false;

		// True if any tenant group name starts with \xff
		state bool hasSystemTenantGroup = false;

		state int newGroups = 0;
		state std::set<TenantGroupName> groupsToCreate;
		for (int i = 0; i < numGroups; ++i) {
			TenantGroupName name = self->chooseTenantGroup(true, false).get();
			while (!groupsToCreate.insert(name).second) {
				name = self->chooseTenantGroup(true, false).get();
			}

			if (self->createdTenantGroups.count(name)) {
				alreadyExists = true;
			}

			++newGroups;
			hasSystemTenantGroup = hasSystemTenantGroup || name.startsWith("\xff"_sr);
		}

		// If any tenant group existed at the start of this function, then we expect the creation to fail or be a no-op,
		// depending on the type of create operation being executed
		state bool existedAtStart = alreadyExists;
		state int64_t minGroupCount = std::numeric_limits<int64_t>::max();
		state int64_t finalGroupCount = 0;

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->dataDb);

		state Version originalReadVersion = wait(self->getLatestReadVersion(self, operationType));
		// First, attempt to create the tenants
		state bool retried = false;
		loop {
			try {
				if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
					tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					wait(store(finalGroupCount, TenantMetadata::tenantGroupCount().getD(tr, Snapshot::False, 0)));
					minGroupCount = std::min(finalGroupCount, minGroupCount);
				}

				// Attempt to read the chosen list of tenant groups
				Optional<Void> result = wait(timeout(createTenantGroupImpl(tr, groupsToCreate, operationType, self),
				                                     deterministicRandom()->randomInt(1, 30)));

				if (result.present()) {
					// Make sure that we had capacity to create the tenant groups. This cannot be validated for
					// database operations because we cannot determine the tenant group count in the same
					// transaction that the tenant group is created
					if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
						ASSERT(minGroupCount + newGroups <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER);
					} else {
						// Database operations shouldn't get here if the tenant group already exists
						ASSERT(!alreadyExists);
					}

					break;
				}

				retried = true;
				tr->reset();
			} catch (Error& e) {
				// If we retried the creation after our initial attempt succeeded, then we proceed with the rest
				// of the creation steps normally. Otherwise, the creation happened elsewhere and we failed
				// here, so we can rethrow the error.
				if (e.code() == error_code_tenant_group_already_exists && !existedAtStart) {
					ASSERT(operationType == OperationType::METACLUSTER ||
					       operationType == OperationType::MANAGEMENT_DATABASE);
					ASSERT(retried);
					break;
				} else if (e.code() == error_code_invalid_tenant_group_name) {
					ASSERT(hasSystemTenantGroup);
					return Void();
				} else if (e.code() == error_code_invalid_metacluster_operation) {
					ASSERT(operationType == OperationType::METACLUSTER != self->useMetacluster);
					return Void();
				} else if (e.code() == error_code_cluster_no_capacity) {
					// Confirm that we overshot our capacity. This check cannot be done for database operations
					// because we cannot transactionally get the tenant count with the creation.
					if (operationType == OperationType::MANAGEMENT_TRANSACTION) {
						ASSERT(finalGroupCount + newGroups > CLIENT_KNOBS->MAX_TENANT_GROUPS_PER_CLUSTER);
					}
					return Void();
				}

				// Database-based operations should not need to be retried
				else if (operationType == OperationType::MANAGEMENT_DATABASE ||
				         operationType == OperationType::METACLUSTER) {
					if (e.code() == error_code_tenant_group_already_exists) {
						ASSERT(existedAtStart);
					} else {
						ASSERT(groupsToCreate.size() == 1);
						TraceEvent(SevError, "CreateTenantGroupFailure")
						    .error(e)
						    .detail("GroupName", *groupsToCreate.begin());
					}
					return Void();
				}

				// Transaction-based operations should be retried
				else {
					try {
						wait(tr->onError(e));
					} catch (Error& e) {
						for (auto const& group : groupsToCreate) {
							TraceEvent(SevError, "CreateTenantGroupFailure").error(e).detail("GroupName", group);
						}
						return Void();
					}
				}
			}

			// Check the state of the first created tenant group
			Optional<TenantGroupEntry> resultEntry =
			    wait(self->tryGetTenantGroup(*groupsToCreate.begin(), operationType));

			if (resultEntry.present()) {
				if (resultEntry.get().groupState == TenantState::READY) {
					// The tenant now exists, so we will retry and expect the creation to react accordingly
					alreadyExists = true;
				} else {
					// Only a metacluster tenant creation can end up in a partially created state
					// We should be able to retry and pick up where we left off
					ASSERT(operationType == OperationType::METACLUSTER);
					ASSERT(resultEntry.get().groupState == TenantState::REGISTERING);
				}
			}
		}

		// Check that using the wrong creation type fails depending on whether we are using a metacluster
		ASSERT(self->useMetacluster == (operationType == OperationType::METACLUSTER));

		// Database-based creation modes will fail if the tenant group already existed
		if (operationType == OperationType::MANAGEMENT_DATABASE || operationType == OperationType::METACLUSTER) {
			ASSERT(!existedAtStart);
		}

		// It is not legal to create a tenant group starting with \xff
		ASSERT(!hasSystemTenantGroup);

		state std::set<TenantGroupName>::iterator groupItr;
		for (groupItr = groupsToCreate.begin(); groupItr != groupsToCreate.end(); ++groupItr) {
			// Ignore any tenant groups that already existed
			if (self->createdTenantGroups.count(*groupItr)) {
				fmt::print("Tenant group already created: {}\n", printable(*groupItr));
				continue;
			}

			// Read the created tenant group object and verify that its state is correct
			state Optional<TenantGroupEntry> entry = wait(self->tryGetTenantGroup(*groupItr, operationType));

			ASSERT(entry.present());
			ASSERT_GT(entry.get().id, self->maxId);
			ASSERT_EQ(entry.get().groupState, TenantState::READY);

			Versionstamp currentVersionstamp = wait(getLastTenantModification(self, operationType));
			ASSERT_GT(currentVersionstamp.version, originalReadVersion);

			if (self->useMetacluster) {
				// In a metacluster, we should also see that the tenant was created on the data cluster
				Optional<TenantGroupEntry> dataEntry =
				    wait(TenantAPI::tryGetTenantGroup(self->dataDb.getReference(), *groupItr));
				ASSERT(dataEntry.present());
				ASSERT_EQ(dataEntry.get().id, entry.get().id);
				ASSERT_EQ(dataEntry.get().groupState, TenantState::READY);
			}

			// Update our local tenant state to include the newly created one
			self->maxId = entry.get().id;
			self->createdTenantGroups[*groupItr] = TenantGroupData(entry.get().id);
		}

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, TenantManagementWorkload* self) {
		state double start = now();

		// Run a random sequence of tenant management operations for the duration of the test
		while (now() < start + self->testDuration) {
			state int operation = deterministicRandom()->randomInt(0, 11);
			if (operation == 0) {
				wait(createTenant(self));
			} else if (operation == 1) {
				wait(deleteTenant(self));
			} else if (operation == 2) {
				wait(getTenant(self));
			} else if (operation == 3) {
				wait(listTenants(self));
			} else if (operation == 4) {
				wait(renameTenant(self));
			} else if (operation == 5) {
				wait(configureTenant(self));
			} else if (operation == 6) {
				wait(getTenantGroup(self));
			} else if (operation == 7) {
				wait(listTenantGroups(self));
			} else if (operation == 8) {
				// wait(createTenantGroup(self));
			} else if (operation == 9) {
				// wait(deleteTenantGroup(self));
			} else if (operation == 10) {
				wait(readTenantKey(self));
			}
		}

		return Void();
	}

	// Verify that the set of tenants in the database matches our local state
	ACTOR static Future<Void> compareTenants(TenantManagementWorkload* self) {
		state std::map<TenantName, TenantData>::iterator localItr = self->createdTenants.begin();
		state std::vector<Future<Void>> checkTenants;
		state TenantName beginTenant = ""_sr.withPrefix(self->localTenantNamePrefix);
		state TenantName endTenant = "\xff\xff"_sr.withPrefix(self->localTenantNamePrefix);

		loop {
			// Read the tenant list from the data cluster.
			state std::vector<std::pair<TenantName, TenantMapEntry>> dataClusterTenants =
			    wait(TenantAPI::listTenantMetadata(self->dataDb.getReference(), beginTenant, endTenant, 1000));

			auto dataItr = dataClusterTenants.begin();

			TenantNameRef lastTenant;
			while (dataItr != dataClusterTenants.end()) {
				ASSERT(localItr != self->createdTenants.end());
				ASSERT(dataItr->first == localItr->first);
				if (localItr->second.tenantGroup.present()) {
					ASSERT(dataItr->second.tenantGroup ==
					       self->createdTenantGroups[localItr->second.tenantGroup.get()].id);
				} else {
					ASSERT(!dataItr->second.tenantGroup.present());
				}

				checkTenants.push_back(checkTenantContents(self, dataItr->first, localItr->second));
				lastTenant = dataItr->first;

				++localItr;
				++dataItr;
			}

			if (dataClusterTenants.size() < 1000) {
				break;
			} else {
				beginTenant = keyAfter(lastTenant);
			}
		}

		ASSERT(localItr == self->createdTenants.end());
		wait(waitForAll(checkTenants));
		return Void();
	}

	// Verify that the set of tenant groups in the database matches our local state
	ACTOR static Future<Void> compareTenantGroups(TenantManagementWorkload* self) {
		state std::map<TenantName, TenantGroupData>::iterator localItr = self->createdTenantGroups.begin();
		state TenantGroupName beginTenantGroup = ""_sr.withPrefix(self->localTenantGroupNamePrefix);
		state TenantGroupName endTenantGroup = "\xff\xff"_sr.withPrefix(self->localTenantGroupNamePrefix);

		state Reference<ReadYourWritesTransaction> tr = self->dataDb->createTransaction();
		loop {
			state std::vector<std::pair<TenantGroupName, int64_t>> dataClusterTenantGroups;
			loop {
				try {
					// Read the tenant group list from the data cluster.
					store(dataClusterTenantGroups,
					      TenantAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, 1000));
					state std::vector<Future<Optional<TenantGroupEntry>>> groupFutures;
					for (auto const& [name, id] : dataClusterTenantGroups) {
						groupFutures.push_back(TenantAPI::tryGetTenantGroupTransaction(tr, id));
					}
					wait(waitForAll(groupFutures));

					auto dataItr = groupFutures.begin();
					while (dataItr != groupFutures.end()) {
						ASSERT(dataItr->get().present());
						ASSERT(localItr != self->createdTenantGroups.end());
						ASSERT(dataItr->get().get().name == localItr->first);
						ASSERT(!dataItr->get().get().assignedCluster.present());
						beginTenantGroup = keyAfter(dataItr->get().get().name);

						++localItr;
						++dataItr;
					}
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			if (dataClusterTenantGroups.size() < 1000) {
				break;
			}
		}

		ASSERT(localItr == self->createdTenantGroups.end());
		return Void();
	}

	// Check that the tenant tombstones are properly cleaned up
	ACTOR static Future<Void> checkTombstoneCleanup(TenantManagementWorkload* self) {
		state Reference<ReadYourWritesTransaction> tr = self->dataDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				Optional<TenantTombstoneCleanupData> tombstoneCleanupData =
				    wait(TenantMetadata::tombstoneCleanupData().get(tr));

				if (self->oldestDeletionVersion != 0) {
					ASSERT(tombstoneCleanupData.present());
					if (self->newestDeletionVersion - self->oldestDeletionVersion >
					    CLIENT_KNOBS->TENANT_TOMBSTONE_CLEANUP_INTERVAL * CLIENT_KNOBS->VERSIONS_PER_SECOND) {
						ASSERT(tombstoneCleanupData.get().tombstonesErasedThrough >= 0);
					}
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0 || !singleClient) {
			return _check(cx, this);
		} else {
			return true;
		}
	}

	ACTOR static Future<bool> _check(Database cx, TenantManagementWorkload* self) {
		wait(checkNonTenantKey(self));
		wait(compareTenants(self) && compareTenantGroups(self));

		if (self->useMetacluster) {
			// The metacluster consistency check runs the tenant consistency check for each cluster
			state MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
			    self->mvDb, AllowPartialMetaclusterOperations::False);
			wait(metaclusterConsistencyCheck.run());
			wait(checkTombstoneCleanup(self));
		} else {
			state TenantConsistencyCheck<DatabaseContext> tenantConsistencyCheck(self->dataDb.getReference());
			wait(tenantConsistencyCheck.run());
		}

		return true;
	}

	ACTOR static Future<Void> checkNonTenantKey(const TenantManagementWorkload* self) {
		if (!self->hasNoTenantKey)
			return Void();

		state Transaction tr(self->dataDb);

		// Check that the key we set outside of the tenant is present and has the correct value
		// This is the same key we set inside some of our tenants, so this checks that no tenant
		// writes accidentally happened in the raw key-space
		loop {
			try {
				tr.setOption(FDBTransactionOptions::RAW_ACCESS);
				Optional<Value> val = wait(tr.get(self->keyName));
				ASSERT(val.present() && val.get() == self->noTenantValue);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantManagementWorkload> TenantManagementWorkload;
