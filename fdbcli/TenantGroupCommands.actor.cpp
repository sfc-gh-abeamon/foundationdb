/*
 * TenantGroupCommands.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Schemas.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

Optional<std::map<Standalone<StringRef>, Optional<Value>>>
parseTenantGroupConfiguration(std::vector<StringRef> const& tokens, int startIndex, bool allowUnset) {
	std::map<Standalone<StringRef>, Optional<Value>> configParams;
	for (int tokenNum = startIndex; tokenNum < tokens.size(); ++tokenNum) {
		Optional<Value> value;

		StringRef token = tokens[tokenNum];
		StringRef param;
		if (allowUnset && token == "unset"_sr) {
			if (++tokenNum == tokens.size()) {
				fmt::print(stderr, "ERROR: `unset' specified without a configuration parameter.\n");
				return {};
			}
			param = tokens[tokenNum];
		} else {
			bool foundEquals;
			param = token.eat("=", &foundEquals);
			if (!foundEquals) {
				fmt::print(stderr,
				           "ERROR: invalid configuration string `{}'. String must specify a value using `='.\n",
				           param.toString().c_str());
				return {};
			}
			value = token;
		}

		if (configParams.count(param)) {
			fmt::print(
			    stderr, "ERROR: configuration parameter `{}' specified more than once.\n", param.toString().c_str());
			return {};
		}

		if (tokencmp(param, "assigned_cluster")) {
			configParams[param] = value;
		} else {
			fmt::print(stderr, "ERROR: unrecognized configuration parameter `{}'.\n", param.toString().c_str());
			return {};
		}
	}

	return configParams;
}

// tenantgroup create command
ACTOR Future<bool> tenantGroupCreateCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() < 3 || tokens.size() > 4) {
		fmt::print("Usage: tenantgroup create <NAME> [assigned_cluster=<CLUSTER_NAME>]\n\n");
		fmt::print("Creates a new tenant group in the cluster with the specified name.\n");
		fmt::print("An optional cluster name can be specified that this tenant group will be placed in.\n");
		return false;
	}

	state Reference<ITransaction> tr = db->createTransaction();
	state bool doneExistenceCheck = false;

	state Optional<std::map<Standalone<StringRef>, Optional<Value>>> configuration =
	    parseTenantGroupConfiguration(tokens, 3, false);

	if (!configuration.present()) {
		return false;
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state TenantGroupEntry groupEntry;
			groupEntry.name = tokens[2];
			AssignClusterAutomatically assignClusterAutomatically = AssignClusterAutomatically::True;

			for (auto const& [name, value] : configuration.get()) {
				if (name == "assigned_cluster"_sr) {
					assignClusterAutomatically = AssignClusterAutomatically::False;
					groupEntry.configure(name, value);
				}
			}

			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::createTenantGroup(db, groupEntry, assignClusterAutomatically));
			} else {
				if (!doneExistenceCheck) {
					Optional<int64_t> tenantGroupId = wait(TenantMetadata::tenantGroupNameIndex().get(tr, tokens[2]));
					if (tenantGroupId.present()) {
						throw tenant_group_already_exists();
					}
					doneExistenceCheck = true;
				}

				int64_t nextId = wait(TenantAPI::getNextTenantId(tr));
				TenantMetadata::lastTenantId().set(tr, nextId);

				wait(success(TenantAPI::createTenantGroupTransaction(tr, groupEntry)));
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// tenantgroup delete command
ACTOR Future<bool> tenantGroupDeleteCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 3) {
		fmt::print("Usage: tenantgroup delete <NAME>\n\n");
		fmt::print("Deletes a tenant group from the cluster by name.\n");
		fmt::print("Deletion will be allowed only if the specified tenant group contains no tenants.\n");
		return false;
	}

	state Reference<ITransaction> tr = db->createTransaction();
	state Optional<int64_t> tenantGroupId;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(MetaclusterAPI::deleteTenant(db, tokens[2]));
			} else {
				wait(safeThreadFutureToFuture(tr->commit()));
				if (!tenantGroupId.present()) {
					wait(store(tenantGroupId, TenantMetadata::tenantGroupNameIndex().get(tr, tokens[2])));
					if (!tenantGroupId.present()) {
						throw tenant_not_found();
					}
				}

				wait(TenantAPI::deleteTenantGroupTransaction(tr, tenantGroupId.get()));
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// tenantgroup list command
ACTOR Future<bool> tenantGroupListCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 5) {
		fmt::print("Usage: tenantgroup list [BEGIN] [END] [LIMIT]\n\n");
		fmt::print("Lists the tenant groups in a cluster.\n");
		fmt::print("Only tenant groups in the range BEGIN - END will be printed.\n");
		fmt::print("An optional LIMIT can be specified to limit the number of results (default 100).\n");
		return false;
	}

	state StringRef beginTenantGroup = ""_sr;
	state StringRef endTenantGroup = "\xff\xff"_sr;
	state int limit = 100;

	if (tokens.size() >= 3) {
		beginTenantGroup = tokens[2];
	}
	if (tokens.size() >= 4) {
		endTenantGroup = tokens[3];
		if (endTenantGroup <= beginTenantGroup) {
			fmt::print(stderr, "ERROR: end must be larger than begin");
			return false;
		}
	}
	if (tokens.size() == 5) {
		int n = 0;
		if (sscanf(tokens[4].toString().c_str(), "%d%n", &limit, &n) != 1 || n != tokens[4].size() || limit <= 0) {
			fmt::print(stderr, "ERROR: invalid limit `{}'\n", tokens[4].toString());
			return false;
		}
	}

	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::vector<TenantGroupName> tenantGroupNames;
			state std::vector<std::pair<TenantGroupName, int64_t>> tenantGroups;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(store(tenantGroups,
				           MetaclusterAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
			} else {
				wait(store(tenantGroups,
				           TenantAPI::listTenantGroupsTransaction(tr, beginTenantGroup, endTenantGroup, limit)));
			}

			if (tenantGroups.empty()) {
				if (tokens.size() == 2) {
					fmt::print("The cluster has no tenant groups\n");
				} else {
					fmt::print("The cluster has no tenant groups in the specified range\n");
				}
			}

			int index = 0;
			for (auto tenantGroup : tenantGroups) {
				fmt::print("  {}. {}\n", ++index, printable(tenantGroup.first));
			}

			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// tenantgroup get command
ACTOR Future<bool> tenantGroupGetCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() > 4 || (tokens.size() == 4 && tokens[3] != "JSON"_sr)) {
		fmt::print("Usage: tenantgroup get <NAME> [JSON]\n\n");
		fmt::print("Prints metadata associated with the given tenant group.\n");
		fmt::print("If JSON is specified, then the output will be in JSON format.\n");
		return false;
	}

	state bool useJson = tokens.size() == 4;
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state ClusterType clusterType = wait(TenantAPI::getClusterType(tr));
			state std::string tenantJson;
			state Optional<TenantGroupEntry> entry;
			if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
				wait(store(entry, MetaclusterAPI::tryGetTenantGroupTransaction(tr, tokens[2])));
			} else {
				wait(store(entry, TenantAPI::tryGetTenantGroupTransaction(tr, tokens[2])));
				Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
				    wait(MetaclusterMetadata::metaclusterRegistration().get(tr));

				// We don't store assigned clusters in the tenant group entry on data clusters, so we can instead
				// populate it from the metacluster registration
				if (entry.present() && metaclusterRegistration.present() &&
				    metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA &&
				    !entry.get().assignedCluster.present()) {
					entry.get().assignedCluster = metaclusterRegistration.get().name;
				}
			}

			if (!entry.present()) {
				throw tenant_not_found();
			}

			if (useJson) {
				json_spirit::mObject resultObj;
				resultObj["tenant_group"] = entry.get().toJson();
				resultObj["type"] = "success";
				fmt::print("{}\n",
				           json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print));
			} else {
				if (entry.get().assignedCluster.present()) {
					fmt::print("  assigned cluster: {}\n", printable(entry.get().assignedCluster));
				} else {
					// This is a placeholder output for when a tenant group is read in a non-metacluster, where
					// it currently has no metadata. When metadata is eventually added, we can print that instead.
					fmt::print("The tenant group is present in the cluster\n");
				}
			}
			return true;
		} catch (Error& e) {
			try {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			} catch (Error& finalErr) {
				state std::string errorStr;
				if (finalErr.code() == error_code_tenant_not_found) {
					errorStr = "tenant group not found";
				} else if (useJson) {
					errorStr = finalErr.what();
				} else {
					throw finalErr;
				}

				if (useJson) {
					json_spirit::mObject resultObj;
					resultObj["type"] = "error";
					resultObj["error"] = errorStr;
					fmt::print("{}\n",
					           json_spirit::write_string(json_spirit::mValue(resultObj), json_spirit::pretty_print));
				} else {
					fmt::print(stderr, "ERROR: {}\n", errorStr);
				}

				return false;
			}
		}
	}
}

// tenantgroup command
Future<bool> tenantGroupCommand(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return true;
	} else if (tokencmp(tokens[1], "create")) {
		return tenantGroupCreateCommand(db, tokens);
	} else if (tokencmp(tokens[1], "delete")) {
		return tenantGroupDeleteCommand(db, tokens);
	} else if (tokencmp(tokens[1], "list")) {
		return tenantGroupListCommand(db, tokens);
	} else if (tokencmp(tokens[1], "get")) {
		return tenantGroupGetCommand(db, tokens);
	} else {
		printUsage(tokens[0]);
		return true;
	}
}

void tenantGroupGenerator(const char* text,
                          const char* line,
                          std::vector<std::string>& lc,
                          std::vector<StringRef> const& tokens) {
	if (tokens.size() == 1) {
		const char* opts[] = { "create", "delete", "list", "get", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 3 && tokencmp(tokens[1], "get")) {
		const char* opts[] = { "JSON", nullptr };
		arrayGenerator(text, line, opts, lc);
	}
}

std::vector<const char*> tenantGroupHintGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<create|delete|list|get>", "[ARGS]" };
	} else if (tokencmp(tokens[1], "create") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "[assigned_cluster=<CLUSTER_NAME>]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "delete") && tokens.size() < 3) {
		static std::vector<const char*> opts = { "<NAME>" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "list") && tokens.size() < 5) {
		static std::vector<const char*> opts = { "[BEGIN]", "[END]", "[LIMIT]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else if (tokencmp(tokens[1], "get") && tokens.size() < 4) {
		static std::vector<const char*> opts = { "<NAME>", "[JSON]" };
		return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
	} else {
		return {};
	}
}

CommandFactory tenantGroupRegisterFactory(
    "tenantgroup",
    CommandHelp("tenantgroup <create|delete|list|get> [ARGS]",
                "view and manage tenant groups in a cluster or metacluster",
                "`create' and `delete' add and remove tenant groups from the cluster.\n"
                "`list' prints a list of tenant groups in the cluster.\n"
                "`get' prints the metadata for a particular tenant group.\n"),
    &tenantGroupGenerator,
    &tenantGroupHintGenerator);

} // namespace fdb_cli
