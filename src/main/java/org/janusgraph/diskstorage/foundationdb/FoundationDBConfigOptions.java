// Copyright 2018 Expero Inc., 2023 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.util.List;

/**
 * Configuration options for the FoundationDB storage backend.
 * These are managed under the 'fdb' namespace in the configuration.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 */
@PreInitializeConfigOptions
public interface FoundationDBConfigOptions {

    ConfigNamespace FDB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "fdb",
        "FoundationDB storage backend options");

    ConfigOption<String> DIRECTORY = new ConfigOption<>(
        FDB_NS,
        "directory",
        "The name of the JanusGraph directory (or subspace path) in FoundationDB. It will be created if it does not exist.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    ConfigOption<Integer> VERSION = new ConfigOption<>(
        FDB_NS,
        "version",
        "The FoundationDB API version to use. Ensure this matches your FDB client and server capabilities.",
        ConfigOption.Type.LOCAL,
        // Default to a version compatible with modern clients (e.g., 710 for 7.1.x, 730 for 7.3.x)
        // Check FDB documentation for the specific version number required by your client library version.
        730);

    ConfigOption<String> CLUSTER_FILE_PATH = new ConfigOption<>(
        FDB_NS,
        "cluster-file-path",
        "Path to the FoundationDB cluster file. 'default' uses the default path.",
        ConfigOption.Type.LOCAL,
        "default");

    ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
        FDB_NS,
        "isolation-level",
        "Transaction isolation level. Options: 'serializable' (strongest consistency, potential for more conflicts), " +
            "'read_committed_no_write', 'read_committed_with_write' (allow reads during write transactions, may require retries). " +
            "Default 'serializable' is safest. For high-throughput/low-latency read-heavy workloads, consider 'read_committed_no_write'.",
        ConfigOption.Type.LOCAL,
        "serializable", // Keep default as serializable for safety, recommend tuning
        s -> s != null && List.of("serializable", "read_committed_no_write", "read_committed_with_write").contains(s.toLowerCase().trim()));

    ConfigOption<String> GET_RANGE_MODE = new ConfigOption<>(
        FDB_NS,
        "get-range-mode",
        "The mode for executing FDB getRange operations. 'iterator' (async, default, generally preferred for performance) " +
            "or 'list' (sync, loads all results into memory).",
        ConfigOption.Type.LOCAL,
        "iterator", // Default to async iterator mode
        s -> s != null && List.of("iterator", "list").contains(s.toLowerCase().trim()));

    ConfigOption<Integer> MAX_RETRY_ATTEMPTS = new ConfigOption<>(
        FDB_NS,
        "max-retry-attempts",
        "Maximum number of times to retry a transaction that fails due to a potentially transient FDBException (e.g., conflicts). Only applicable for non-serializable isolation levels.",
        ConfigOption.Type.LOCAL,
        5); // Provide a sensible default

    ConfigOption<Long> RETRY_BACKOFF_MILLIS = new ConfigOption<>(
        FDB_NS,
        "retry-backoff-millis",
        "Initial backoff delay in milliseconds before retrying a failed transaction. Increases exponentially. Only applicable for non-serializable isolation levels.",
        ConfigOption.Type.LOCAL,
        20L); // Provide a small initial backoff
}
