/*
 * DataMovementBetweenClusters.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/flow.h"
#include <string>
#include "flow/actorcompiler.h" // This must be the last #include.

struct DataMovementBetweenClusters : TestWorkload {
	Database extraDB;

	explicit DataMovementBetweenClusters(const WorkloadContext& wcx) : TestWorkload(wcx) {
		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);
	}

	std::string description() const override { return "DataMovementBetweenClusters"; }

	ACTOR static Future<Void> insertTestData(Database cx) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state std::vector<std::string> prefixes = std::vector<std::string>{ "a", "b", "c" };

		loop {
			try {
				for (const auto& prefix : prefixes) {
					tr->set("_Key"_sr.withPrefix(prefix), Value("TestValue_" + prefix));
				}
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return Void();
	}

	Future<Void> setup(const Database& cx) override {
		if (clientId != 0)
			return Void();
		return _setup(this, cx);
	}

	ACTOR static Future<Void> _setup(DataMovementBetweenClusters* self, Database cx) {
		wait(insertTestData(cx));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(DataMovementBetweenClusters* self, Database cx) {
		state KeyRef targetPrefix = "b"_sr;

		// Start the movement
		MoveTenantToClusterReply startReply = wait(sendTenantBalancerRequest(
		    cx,
		    MoveTenantToClusterRequest(
		        targetPrefix, targetPrefix, self->extraDB->getConnectionRecord()->getConnectionString().toString()),
		    &TenantBalancerInterface::moveTenantToCluster));

		// Wait until the movement is ready to switch
		loop {
			GetMovementStatusReply statusReply =
			    wait(sendTenantBalancerRequest(cx,
			                                   GetMovementStatusRequest(targetPrefix, MovementLocation::SOURCE),
			                                   &TenantBalancerInterface::getMovementStatus));

			if (statusReply.movementStatus.tenantMovementInfo.movementState == MovementState::READY_FOR_SWITCH) {
				break;
			}

			wait(delay(1.0));
		}

		// Finish the movement
		FinishSourceMovementReply finishReply =
		    wait(sendTenantBalancerRequest(cx,
		                                   FinishSourceMovementRequest(targetPrefix, Optional<double>()),
		                                   &TenantBalancerInterface::finishSourceMovement));

		// clean
		CleanupMovementSourceReply cleanReply = wait(sendTenantBalancerRequest(
		    cx,
		    CleanupMovementSourceRequest(targetPrefix, CleanupMovementSourceRequest::CleanupType::ERASE_AND_UNLOCK),
		    &TenantBalancerInterface::cleanupMovementSource));

		return Void();
	}

	ACTOR static Future<bool> _check(DataMovementBetweenClusters* self, Database cx) {
		state Reference<ReadYourWritesTransaction> trSrc = makeReference<ReadYourWritesTransaction>(cx);

		// The source database should have prefixes "a" and "c"
		loop {
			try {

				RangeResult result = wait(trSrc->getRange(KeyRangeRef(""_sr, "\xff"_sr), 3));
				ASSERT(result.size() == 2);
				ASSERT(result[0].key == "a_Key"_sr && result[1].key == "c_Key"_sr);
				break;
			} catch (Error& e) {
				wait(trSrc->onError(e));
			}
		}

		state Reference<ReadYourWritesTransaction> trDest = makeReference<ReadYourWritesTransaction>(self->extraDB);

		// The destination database should have prefix "b"
		loop {
			try {

				RangeResult result = wait(trDest->getRange(KeyRangeRef(""_sr, "\xff"_sr), 2));
				ASSERT(result.size() == 1);
				ASSERT(result[0].key == "b_Key"_sr);
				break;
			} catch (Error& e) {
				wait(trSrc->onError(e));
			}
		}

		return true;
	}

	Future<bool> check(const Database& cx) override { return _check(this, cx); }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

REGISTER_WORKLOAD(DataMovementBetweenClusters);
