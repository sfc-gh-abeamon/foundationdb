/*
 * TenantBalancer.actor.cpp
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Trace.h"
#include "fdbclient/StatusClient.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <vector>

class SourceMovementRecord {
public:
	SourceMovementRecord() {}
	SourceMovementRecord(Standalone<StringRef> sourcePrefix,
	                     Standalone<StringRef> destinationPrefix,
	                     std::string databaseName,
	                     Database destinationDb)
	  : sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix), databaseName(databaseName),
	    destinationDb(destinationDb), tagName(databaseName) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }
	Database getDestinationDatabase() const { return destinationDb; }
	std::string getTagName() const { return tagName; }

private:
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;

	std::string databaseName;
	// TODO: leave this open, or open it at request time?
	Database destinationDb;
	std::string tagName;
};

class DestinationMovementRecord {
public:
	DestinationMovementRecord() {}
	DestinationMovementRecord(Standalone<StringRef> sourcePrefix, Standalone<StringRef> destinationPrefix)
	  : sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }

private:
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;
};

ACTOR static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                            Reference<AsyncVar<ClientDBInfo>> info) {
	loop {
		ClientDBInfo clientInfo = dbInfo->get().client;
		info->set(clientInfo);
		wait(dbInfo->onChange());
	}
}

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : tbi(tbi), dbInfo(dbInfo), actors(false) {
		auto info = makeReference<AsyncVar<ClientDBInfo>>();
		db = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::False, EnableLocalityLoadBalance::True);

		agent = DatabaseBackupAgent(db);
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	Database db;

	ActorCollection actors;
	DatabaseBackupAgent agent;

	SourceMovementRecord getOutgoingMovement(Key prefix) const {
		auto itr = outgoingMovements.find(prefix);
		if (itr == outgoingMovements.end()) {
			throw movement_not_found();
		}

		return itr->second;
	}

	DestinationMovementRecord getIncomingMovement(Key prefix) const {
		auto itr = incomingMovements.find(prefix);
		if (itr == incomingMovements.end()) {
			throw movement_not_found();
		}

		return itr->second;
	}

	void saveOutgoingMovement(SourceMovementRecord const& record) {
		outgoingMovements[record.getSourcePrefix()] = record;
		// TODO: persist in DB
	}

	void saveIncomingMovement(DestinationMovementRecord const& record) {
		incomingMovements[record.getDestinationPrefix()] = record;
		// TODO: persist in DB
	}

	bool hasSourceMovement(Key prefix) const { return outgoingMovements.count(prefix) > 0; }
	bool hasDestinationMovement(Key prefix) const { return incomingMovements.count(prefix) > 0; }

	// Returns true if the database is inserted or matches the existing entry
	bool addExternalDatabase(std::string name, std::string connectionString) {
		auto itr = externalDatabases.find(name);
		if (itr != externalDatabases.end()) {
			return itr->second->getConnectionRecord()->getConnectionString().toString() == connectionString;
		}

		// TODO: use a key-backed connection file
		externalDatabases[name] = Database::createDatabase(
		    makeReference<ClusterConnectionMemoryRecord>(ClusterConnectionString(connectionString)),
		    Database::API_VERSION_LATEST,
		    IsInternal::True,
		    tbi.locality);

		return true;
	}

	Optional<Database> getExternalDatabase(std::string name) const {
		auto itr = externalDatabases.find(name);
		if (itr == externalDatabases.end()) {
			return Optional<Database>();
		}

		return itr->second;
	}

private:
	std::unordered_map<std::string, Database> externalDatabases;
	std::map<Key, SourceMovementRecord> outgoingMovements;
	std::map<Key, DestinationMovementRecord> incomingMovements;
};

// src
ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code
	try {
		// 1.Extract necessary data from metadata
		self->addExternalDatabase(req.destConnectionString, req.destConnectionString);
		state Database destDatabase = self->getExternalDatabase(req.destConnectionString).get();
		state SourceMovementRecord sourceMovementRecord(
		    req.sourcePrefix, req.destPrefix, req.destConnectionString, destDatabase);
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.add(prefixRange(req.sourcePrefix));

		// 2.Use DR to do datamovement
		wait(self->agent.submitBackup(self->getExternalDatabase(req.destConnectionString).get(),
		                              KeyRef(sourceMovementRecord.getTagName()),
		                              backupRanges,
		                              StopWhenDone::False,
		                              req.destPrefix,
		                              req.sourcePrefix,
		                              LockDB::False));
		// Check if a backup agent is running
		bool agentRunning = wait(self->agent.checkActive(destDatabase));
		if (!agentRunning) {
			printf("The data movement on%s was successfully submitted but no DR agents are responding.\n",
			       self->db->getConnectionRecord()->getConnectionString().toString().c_str());
			// Throw an error that will not display any additional information
			throw actor_cancelled();
		}

		// 3.Do record
		self->saveOutgoingMovement(sourceMovementRecord);

		MoveTenantToClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

// dest
ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code
	try {
		Key targetPrefix = req.destPrefix;

		// 1.Lock the destination before we start the movement
		// TODO

		// 2.Check if prefix is empty.
		bool isPrefixEmpty = wait(self->agent.isTenantEmpty(self->db, targetPrefix));
		if (!isPrefixEmpty) {
			throw movement_dest_prefix_no_empty();
		}

		// 3.Do record
		DestinationMovementRecord destinationMovementRecord(req.sourcePrefix, req.destPrefix);
		self->saveIncomingMovement(destinationMovementRecord);

		ReceiveTenantFromClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<std::vector<TenantMovementInfo>> fetchDBMove(Database db, bool isSrc) {
	state std::vector<TenantMovementInfo> recorder;
	try {
		// TODO distinguish dr and data movement
		// get running data movement
		// TODO make sure is this the right way to get status json?
		// TODO switch to another cheaper way
		state StatusObject statusObjCluster = wait(StatusClient::statusFetcher(db));
		StatusObjectReader reader(statusObjCluster);
		std::string context = isSrc ? "dr_backup" : "dr_backup_dest";
		std::string path = format("layers.%s.tags", context.c_str());
		StatusObjectReader tags;
		if (reader.tryGet(path, tags)) {
			for (auto itr : tags.obj()) {
				JSONDoc tag(itr.second);
				bool running = false;
				tag.tryGet("running_backup", running);
				if (running) {
					std::string backup_state, seconds_behind, uid;
					tag.tryGet("backup_state", backup_state);
					tag.tryGet("seconds_behind", seconds_behind);
					tag.tryGet("mutation_stream_id", uid);
					TenantMovementInfo tenantMovementInfo;
					tenantMovementInfo.movementLocation =
					    isSrc ? TenantMovementInfo::Location::SOURCE : TenantMovementInfo::Location::DEST;
					tenantMovementInfo.TenandMovementStatus = backup_state;
					tenantMovementInfo.seconds_behind = seconds_behind;
					tenantMovementInfo.uid = uid;
					recorder.push_back(tenantMovementInfo);
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return recorder;
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		// Get all active movement from status json, and transfer them into TenantMovementInfo in reply
		// TODO acquire the srcPrefix and destPrefix from somewhere
		state std::vector<TenantMovementInfo> statusAsSrc = wait(fetchDBMove(self->db, true));
		state std::vector<TenantMovementInfo> statusAsDest = wait(fetchDBMove(self->db, false));
		GetActiveMovementsReply reply;
		reply.activeMovements.insert(reply.activeMovements.end(), statusAsSrc.begin(), statusAsSrc.end());
		reply.activeMovements.insert(reply.activeMovements.end(), statusAsDest.begin(), statusAsDest.end());
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishSourceMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		// TODO once the range has been unlocked, it will no longer be legal to run cleanup
		CleanupMovementSourceRequest::CleanupType cleanupType = req.cleanupType;
		state std::string tenantName = req.tenantName;
		if (cleanupType != CleanupMovementSourceRequest::CleanupType::UNLOCK) {
			// clear specific tenant
			wait(self->agent.clearPrefix(self->db, Key(tenantName)));
		}
		if (cleanupType != CleanupMovementSourceRequest::CleanupType::ERASE) {
			wait(self->agent.unlockBackup(self->db, Key(tenantName)));
		}
		CleanupMovementSourceReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	loop choose {
		when(MoveTenantToClusterRequest req = waitNext(self->tbi.moveTenantToCluster.getFuture())) {
			self->actors.add(moveTenantToCluster(self, req));
		}
		when(ReceiveTenantFromClusterRequest req = waitNext(self->tbi.receiveTenantFromCluster.getFuture())) {
			self->actors.add(receiveTenantFromCluster(self, req));
		}
		when(GetActiveMovementsRequest req = waitNext(self->tbi.getActiveMovements.getFuture())) {
			self->actors.add(getActiveMovements(self, req));
		}
		when(FinishSourceMovementRequest req = waitNext(self->tbi.finishSourceMovement.getFuture())) {
			self->actors.add(finishSourceMovement(self, req));
		}
		when(FinishDestinationMovementRequest req = waitNext(self->tbi.finishDestinationMovement.getFuture())) {
			self->actors.add(finishDestinationMovement(self, req));
		}
		when(AbortMovementRequest req = waitNext(self->tbi.abortMovement.getFuture())) {
			self->actors.add(abortMovement(self, req));
		}
		when(CleanupMovementSourceRequest req = waitNext(self->tbi.cleanupMovementSource.getFuture())) {
			self->actors.add(cleanupMovementSource(self, req));
		}
		when(wait(self->actors.getResult())) {}
	}
}

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> db) {
	state TenantBalancer self(tbi, db);

	try {
		wait(tenantBalancerCore(&self));
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated").error(e);
		throw e;
	}
}