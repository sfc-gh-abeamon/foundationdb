/*
 * ClusterConnectionFile.actor.cpp
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

#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h" // has to be last include

std::pair<std::string, bool> ClusterConnectionFile::lookupClusterFileName(std::string const& filename) {
	if (filename.length())
		return std::make_pair(filename, false);

	std::string f;
	bool isDefaultFile = true;
	if (platform::getEnvironmentVar(CLUSTER_FILE_ENV_VAR_NAME, f)) {
		// If this is set but points to a file that does not
		// exist, we will not fallback to any other methods
		isDefaultFile = false;
	} else if (fileExists("fdb.cluster"))
		f = "fdb.cluster";
	else
		f = platform::getDefaultClusterFilePath();

	return std::make_pair(f, isDefaultFile);
}

std::string ClusterConnectionFile::getErrorString(std::pair<std::string, bool> const& resolvedClusterFile,
                                                  Error const& e) {
	bool isDefault = resolvedClusterFile.second;
	if (e.code() == error_code_connection_string_invalid) {
		return format("Invalid cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	} else if (e.code() == error_code_no_cluster_file_found) {
		if (isDefault)
			return format("Unable to read cluster file `./fdb.cluster' or `%s' and %s unset: %d %s",
			              platform::getDefaultClusterFilePath().c_str(),
			              CLUSTER_FILE_ENV_VAR_NAME,
			              e.code(),
			              e.what());
		else
			return format(
			    "Unable to read cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	} else {
		return format(
		    "Unexpected error loading cluster file `%s': %d %s", resolvedClusterFile.first.c_str(), e.code(), e.what());
	}
}

ClusterConnectionFile::ClusterConnectionFile(std::string const& filename) : IClusterConnectionRecord(false) {
	if (!fileExists(filename)) {
		throw no_cluster_file_found();
	}

	cs = ClusterConnectionString(readFileBytes(filename, MAX_CLUSTER_FILE_BYTES));
	this->filename = filename;
}

ClusterConnectionFile::ClusterConnectionFile(std::string const& filename, ClusterConnectionString const& contents)
  : IClusterConnectionRecord(true) {
	this->filename = filename;
	cs = contents;
}

ClusterConnectionString const& ClusterConnectionFile::getConnectionString() const {
	return cs;
}

Future<bool> ClusterConnectionFile::upToDate(ClusterConnectionString& fileConnectionString) {
	try {
		// the cluster file hasn't been created yet so there's nothing to check
		if (needsToBePersisted())
			return true;

		ClusterConnectionFile temp(filename);
		fileConnectionString = temp.getConnectionString();
		return fileConnectionString.toString() == cs.toString();
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "ClusterFileError").error(e).detail("Filename", filename);
		return false; // Swallow the error and report that the file is out of date
	}
}

Future<bool> ClusterConnectionFile::persist() {
	setPersisted();

	if (filename.size()) {
		try {
			atomicReplace(filename,
			              "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n" +
			                  cs.toString().append("\n"));

			Future<bool> isUpToDate = IClusterConnectionRecord::upToDate();

			// The implementation of upToDate in this class is synchronous
			ASSERT(isUpToDate.isReady());

			if (!isUpToDate.get()) {
				// This should only happen in rare scenarios where multiple processes are updating the same file to
				// different values simultaneously In that case, we don't have any guarantees about which file will
				// ultimately be written
				TraceEvent(SevWarnAlways, "ClusterFileChangedAfterReplace")
				    .detail("Filename", filename)
				    .detail("ConnStr", cs.toString());
				return false;
			}

			return true;
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToChangeConnectionFile")
			    .error(e)
			    .detail("Filename", filename)
			    .detail("ConnStr", cs.toString());
		}
	}

	return false;
}

Future<Void> ClusterConnectionFile::setConnectionString(ClusterConnectionString const& conn) {
	ASSERT(filename.size());
	cs = conn;
	return success(persist());
}

Future<ClusterConnectionString> ClusterConnectionFile::getStoredConnectionString() {
	return ClusterConnectionFile(filename).cs;
}

Standalone<StringRef> ClusterConnectionFile::getLocation() const {
	return StringRef(filename);
}

Reference<IClusterConnectionRecord> ClusterConnectionFile::makeIntermediateRecord(
    ClusterConnectionString const& connectionString) const {
	return makeReference<ClusterConnectionFile>(filename, connectionString);
}

bool ClusterConnectionFile::isValid() const {
	return filename.size() != 0;
}

std::string ClusterConnectionFile::toString() const {
	if (isValid()) {
		return "File: " + filename;
	} else {
		return "File: <unset>";
	}
}