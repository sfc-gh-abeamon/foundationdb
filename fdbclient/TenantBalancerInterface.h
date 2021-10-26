/*
 * TenantBalancerInterface.h
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

#ifndef FDBCLIENT_TENANTBALANCERINTERFACE_H
#define FDBCLIENT_TENANTBALANCERINTERFACE_H
#include <stdbool.h>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include "fdbclient/JSONDoc.h"

// extern enum class MovementState;
enum class MovementState { INITIALIZING, STARTED, READY_FOR_SWITCH, SWITCHING, COMPLETED, ERROR };
enum class MovementLocation { SOURCE, DEST };

struct TenantBalancerInterface {
	constexpr static FileIdentifier file_identifier = 6185894;

	struct LocalityData locality;
	UID uniqueId;

	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltTenantBalancerRequest> haltTenantBalancer;

	// Start is two separate requests here. This could be made into one if the tenant balancer in the source could talk
	// to the dest
	RequestStream<struct MoveTenantToClusterRequest> moveTenantToCluster;
	RequestStream<struct ReceiveTenantFromClusterRequest> receiveTenantFromCluster;

	RequestStream<struct GetActiveMovementsRequest> getActiveMovements;
	// Right now we get all details from listing all movements. Do we need an individual movement status request?
	RequestStream<struct GetMovementStatusRequest> getMovementStatus;

	// Finish source and dest are two steps here. We may not want this, given that it then becomes the responsibility of
	// the move client to handle failures. We can fix this once the tenant balancers can talk to each other.
	RequestStream<struct FinishSourceMovementRequest> finishSourceMovement;
	RequestStream<struct FinishDestinationMovementRequest> finishDestinationMovement;

	RequestStream<struct RecoverMovementRequest> recoverMovement;

	RequestStream<struct AbortMovementRequest> abortMovement;
	RequestStream<struct CleanupMovementSourceRequest> cleanupMovementSource;

	explicit TenantBalancerInterface(const struct LocalityData& locality, UID uniqueId)
	  : locality(locality), uniqueId(uniqueId) {}
	TenantBalancerInterface() : uniqueId(deterministicRandom()->randomUniqueID()) {}

	static std::string movementStateToString(MovementState state);
	static std::string movementLocationToString(MovementLocation location);

	NetworkAddress address() const { return moveTenantToCluster.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return moveTenantToCluster.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const {
		return moveTenantToCluster.getEndpoint().addresses.secondaryAddress;
	}

	UID id() const { return uniqueId; }
	std::string toString() const { return id().shortString(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           uniqueId,
		           waitFailure,
		           haltTenantBalancer,
		           moveTenantToCluster,
		           receiveTenantFromCluster,
		           getActiveMovements,
		           getMovementStatus,
		           finishSourceMovement,
		           finishDestinationMovement,
		           recoverMovement,
		           abortMovement,
		           cleanupMovementSource);
	}

	bool operator==(TenantBalancerInterface const& s) const { return uniqueId == s.uniqueId; }
	bool operator<(TenantBalancerInterface const& s) const { return uniqueId < s.uniqueId; }

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(moveTenantToCluster.getReceiver());
		streams.push_back(receiveTenantFromCluster.getReceiver());
		streams.push_back(getActiveMovements.getReceiver());
		streams.push_back(getMovementStatus.getReceiver());
		streams.push_back(finishSourceMovement.getReceiver());
		streams.push_back(finishDestinationMovement.getReceiver());
		streams.push_back(recoverMovement.getReceiver());
		streams.push_back(abortMovement.getReceiver());
		streams.push_back(cleanupMovementSource.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct MoveTenantToClusterReply {
	constexpr static FileIdentifier file_identifier = 3708530;

	UID movementId;

	// This is the tenant that was chosen for locking the prefix
	// SOMEDAY: when it is possible that we can specify existing tenants to move, this may look different
	std::string destinationTenantName;

	MoveTenantToClusterReply() {}
	MoveTenantToClusterReply(UID movementId, std::string destinationTenantName)
	  : movementId(movementId), destinationTenantName(destinationTenantName) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, destinationTenantName);
	}
};

struct MoveTenantToClusterRequest {
	constexpr static FileIdentifier file_identifier = 3571712;
	Arena arena;

	KeyRef sourcePrefix;
	KeyRef destPrefix;

	// TODO: dest cluster info
	std::string destConnectionString;

	ReplyPromise<MoveTenantToClusterReply> reply;

	MoveTenantToClusterRequest() {}
	MoveTenantToClusterRequest(KeyRef sourcePrefix, KeyRef destPrefix, std::string destConnectionString)
	  : sourcePrefix(arena, sourcePrefix), destPrefix(arena, destPrefix), destConnectionString(destConnectionString) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sourcePrefix, destPrefix, destConnectionString, reply, arena);
	}
};

struct ReceiveTenantFromClusterReply {
	constexpr static FileIdentifier file_identifier = 2557468;

	// This is the tenant that was chosen for locking the prefix
	// SOMEDAY: when it is possible that we can specify existing tenants to move, this may look different
	std::string tenantName;

	ReceiveTenantFromClusterReply() {}
	ReceiveTenantFromClusterReply(std::string tenantName) : tenantName(tenantName) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantName);
	}
};

struct ReceiveTenantFromClusterRequest {
	constexpr static FileIdentifier file_identifier = 340512;
	Arena arena;

	UID movementId;

	KeyRef sourcePrefix;
	KeyRef destPrefix;

	std::string srcConnectionString;

	ReplyPromise<ReceiveTenantFromClusterReply> reply;

	ReceiveTenantFromClusterRequest() {}
	ReceiveTenantFromClusterRequest(UID movementId,
	                                KeyRef sourcePrefix,
	                                KeyRef destPrefix,
	                                std::string srcConnectionString)
	  : movementId(movementId), sourcePrefix(arena, sourcePrefix), destPrefix(arena, destPrefix),
	    srcConnectionString(srcConnectionString) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, sourcePrefix, destPrefix, srcConnectionString, reply, arena);
	}
};

struct TenantMovementInfo {
	constexpr static FileIdentifier file_identifier = 16510400;

	UID movementId;
	std::string peerConnectionString;
	Key sourcePrefix;
	Key destinationPrefix;
	MovementState movementState;

	TenantMovementInfo() {}
	TenantMovementInfo(UID movementId,
	                   std::string peerConnectionString,
	                   Key sourcePrefix,
	                   Key destinationPrefix,
	                   MovementState movementState)
	  : movementId(movementId), peerConnectionString(peerConnectionString), sourcePrefix(sourcePrefix),
	    destinationPrefix(destinationPrefix), movementState(movementState) {}

	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, peerConnectionString, sourcePrefix, destinationPrefix, movementState);
	}
};

struct TenantMovementStatus {
	constexpr static FileIdentifier file_identifier = 5103586;

	TenantMovementInfo tenantMovementInfo;
	bool isSourceLocked; // Whether the prefix is locked on the source
	bool isDestinationLocked; // Whether the prefix is locked on the destination
	Optional<double> databaseVersionLag; // The number of seconds of lag between the version on the source and the
	                                     // version on the destination
	Optional<double> mutationLag; // The number of seconds of lag between the current mutation on the source and the
	                              // mutations being applied to the destination
	Optional<Version> switchVersion;
	Optional<std::string> errorMessage;

	TenantMovementStatus() {}
	std::string toJson() const;

	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           tenantMovementInfo,
		           isSourceLocked,
		           isDestinationLocked,
		           databaseVersionLag,
		           mutationLag,
		           switchVersion,
		           errorMessage);
	}
};

struct GetActiveMovementsReply {
	constexpr static FileIdentifier file_identifier = 2320458;

	std::vector<TenantMovementInfo> activeMovements;

	GetActiveMovementsReply() {}
	GetActiveMovementsReply(std::vector<TenantMovementInfo> activeMovements) : activeMovements(activeMovements) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, activeMovements);
	}
};

struct GetActiveMovementsRequest {
	constexpr static FileIdentifier file_identifier = 11980148;

	// Filter criteria
	Optional<Key> prefixFilter;
	Optional<std::string> peerDatabaseConnectionStringFilter;
	Optional<MovementLocation> locationFilter;

	ReplyPromise<GetActiveMovementsReply> reply;

	GetActiveMovementsRequest() {}
	GetActiveMovementsRequest(Optional<Key> prefixFilter,
	                          Optional<std::string> peerDatabaseConnectionStringFilter,
	                          Optional<MovementLocation> locationFilter)
	  : prefixFilter(prefixFilter), peerDatabaseConnectionStringFilter(peerDatabaseConnectionStringFilter),
	    locationFilter(locationFilter) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, prefixFilter, peerDatabaseConnectionStringFilter, locationFilter, reply);
	}
};

struct GetMovementStatusReply {
	constexpr static FileIdentifier file_identifier = 4693499;

	TenantMovementStatus movementStatus;

	GetMovementStatusReply() {}
	GetMovementStatusReply(TenantMovementStatus movementStatus) : movementStatus(movementStatus) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementStatus);
	}
};

struct GetMovementStatusRequest {
	constexpr static FileIdentifier file_identifier = 11494877;

	Key prefix;
	MovementLocation movementLocation;

	ReplyPromise<GetMovementStatusReply> reply;

	GetMovementStatusRequest() {}
	GetMovementStatusRequest(Key prefix, MovementLocation movementLocation)
	  : prefix(prefix), movementLocation(movementLocation) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, prefix, movementLocation, reply);
	}
};

struct FinishSourceMovementReply {
	constexpr static FileIdentifier file_identifier = 6276738;

	// Name of locked tenant on source
	std::string sourceTenantName;

	Version version;

	FinishSourceMovementReply() : version(invalidVersion) {}
	FinishSourceMovementReply(std::string sourceTenantName, Version version)
	  : sourceTenantName(sourceTenantName), version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sourceTenantName, version);
	}
};

struct FinishSourceMovementRequest {
	constexpr static FileIdentifier file_identifier = 10934711;

	Key sourcePrefix;
	double maxLagSeconds;

	ReplyPromise<FinishSourceMovementReply> reply;

	FinishSourceMovementRequest() {}
	FinishSourceMovementRequest(Key sourcePrefix, double maxLagSeconds)
	  : sourcePrefix(sourcePrefix), maxLagSeconds(maxLagSeconds) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sourcePrefix, maxLagSeconds, reply);
	}
};

struct FinishDestinationMovementReply {
	constexpr static FileIdentifier file_identifier = 8119999;

	// TODO: do we need any info from this reply?

	FinishDestinationMovementReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct FinishDestinationMovementRequest {
	constexpr static FileIdentifier file_identifier = 12331642;

	UID movementId;
	Key destinationPrefix;
	Version version;

	ReplyPromise<FinishDestinationMovementReply> reply;

	FinishDestinationMovementRequest() : version(invalidVersion) {}
	FinishDestinationMovementRequest(UID movementId, Key destinationPrefix, Version version)
	  : movementId(movementId), destinationPrefix(destinationPrefix), version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, destinationPrefix, version, reply);
	}
};

struct RecoverMovementReply {
	constexpr static FileIdentifier file_identifier = 15462077;

	RecoverMovementReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct RecoverMovementRequest {
	constexpr static FileIdentifier file_identifier = 12114009;

	UID movementId;
	Key prefix;

	ReplyPromise<RecoverMovementReply> reply;

	RecoverMovementRequest() {}
	RecoverMovementRequest(UID movementId, Key prefix) : movementId(movementId), prefix(prefix) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, prefix, reply);
	}
};

struct AbortMovementReply {
	constexpr static FileIdentifier file_identifier = 14761140;

	AbortMovementReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct AbortMovementRequest {
	constexpr static FileIdentifier file_identifier = 14058403;

	Optional<UID> movementId;
	Key prefix;
	MovementLocation movementLocation;

	ReplyPromise<AbortMovementReply> reply;

	AbortMovementRequest() : movementLocation(MovementLocation::SOURCE) {}
	AbortMovementRequest(Key prefix, MovementLocation movementLocation)
	  : prefix(prefix), movementLocation(movementLocation) {}
	AbortMovementRequest(UID movementId, Key prefix, MovementLocation movementLocation)
	  : movementId(movementId), prefix(prefix), movementLocation(movementLocation) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, prefix, movementLocation, reply);
	}
};

struct CleanupMovementSourceReply {
	constexpr static FileIdentifier file_identifier = 14051254;

	// TODO: do we need any info from this reply?

	CleanupMovementSourceReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct CleanupMovementSourceRequest {
	constexpr static FileIdentifier file_identifier = 14718857;

	enum class CleanupType { UNLOCK, ERASE, ERASE_AND_UNLOCK } uint8_t;

	Key prefix;
	CleanupType cleanupType;

	ReplyPromise<CleanupMovementSourceReply> reply;

	CleanupMovementSourceRequest() : cleanupType(CleanupType::UNLOCK) {}
	CleanupMovementSourceRequest(Key prefix, CleanupType cleanupType) : prefix(prefix), cleanupType(cleanupType) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, prefix, cleanupType, reply);
	}
};

struct HaltTenantBalancerRequest {
	constexpr static FileIdentifier file_identifier = 15769279;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltTenantBalancerRequest() {}
	explicit HaltTenantBalancerRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

#endif