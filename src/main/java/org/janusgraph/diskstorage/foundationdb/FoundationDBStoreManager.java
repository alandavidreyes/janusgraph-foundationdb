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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.CLUSTER_FILE_PATH;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.DIRECTORY;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.GET_RANGE_MODE;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.ISOLATION_LEVEL;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.MAX_RETRY_ATTEMPTS;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.RETRY_BACKOFF_MILLIS;
import static org.janusgraph.diskstorage.foundationdb.FoundationDBConfigOptions.VERSION;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;

/**
 * FoundationDB storage manager implementation, optimized for performance.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 * @author JanusGraph Authors
 */
public class FoundationDBStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBStoreManager.class);

    public enum RangeQueryIteratorMode { ASYNC, SYNC }

    private final Map<String, FoundationDBKeyValueStore> stores;

    // FDB API should only be selected once per process
    private static volatile FDB fdb = null;
    private static final Object fdbInitLock = new Object();

    protected Database db;
    protected final StoreFeatures features;
    protected DirectorySubspace rootDirectory;
    protected final String rootDirectoryName;
    protected final FoundationDBTx.IsolationLevel isolationLevel;
    protected final int maxRetryAttempts;
    protected final long retryBackoffMillis;
    private final RangeQueryIteratorMode rangeQueryMode;


    public FoundationDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new ConcurrentHashMap<>();

        synchronized (fdbInitLock) {
            if (fdb == null) {
                int apiVersion = configuration.get(VERSION);
                log.info("Selecting FDB API version: {}", apiVersion);
                // It's crucial to select the API version *before* interacting with FDB
                // FDB.selectAPIVersion throws if called multiple times with different versions
                try {
                    fdb = FDB.selectAPIVersion(apiVersion);
                    // Recommended: Start the FDB network thread explicitly if needed,
                    // though fdb.open() usually handles this.
                    // fdb.startNetwork();
                } catch (FDBException e) {
                    throw new PermanentBackendException("Failed to select FDB API version " + apiVersion +
                        ". Ensure the FDB client libraries are compatible and this is the first initialization.", e);
                } catch (IllegalStateException e) {
                    // This can happen if selectAPIVersion was already called with a different version
                    log.warn("FDB API version may have already been selected. Attempting to continue. Error: {}", e.getMessage());
                    // Try getting the instance if already initialized
                    try {
                        fdb = FDB.instance();
                    } catch (IllegalStateException innerEx) {
                        throw new PermanentBackendException("Failed to initialize FDB. API version conflict or other initialization error.", innerEx);
                    }
                }
            }
        }

        rootDirectoryName = determineRootDirectoryName(configuration);
        String clusterFilePath = configuration.get(CLUSTER_FILE_PATH);

        try {
            log.info("Connecting to FDB cluster file: {}", clusterFilePath);
            db = !"default".equalsIgnoreCase(clusterFilePath) ? fdb.open(clusterFilePath) : fdb.open();
            // Consider setting database-level options if needed (e.g., timeouts)
            // db.options().setTransactionTimeout(...);
            log.info("Connected to FDB cluster.");
        } catch (FDBException e) {
            throw new PermanentBackendException("Failed to open FDB database with cluster file: " + clusterFilePath, e);
        }

        var isolationLevelStr = configuration.get(ISOLATION_LEVEL).toLowerCase().trim();
        isolationLevel = switch (isolationLevelStr) {
            case "serializable" -> FoundationDBTx.IsolationLevel.SERIALIZABLE;
            case "read_committed_no_write" -> FoundationDBTx.IsolationLevel.READ_COMMITTED_NO_WRITE;
            case "read_committed_with_write" -> FoundationDBTx.IsolationLevel.READ_COMMITTED_WITH_WRITE;
            // Should be caught by ConfigOption validation, but handle defensively
            default -> throw new PermanentBackendException("Unrecognized isolation level: " + isolationLevelStr);
        };
        log.info("Using FDB transaction isolation level: {}", isolationLevel);

        var getRangeModeStr = configuration.get(GET_RANGE_MODE).toLowerCase().trim();
        rangeQueryMode = switch (getRangeModeStr) {
            case "iterator" -> RangeQueryIteratorMode.ASYNC;
            case "list" -> RangeQueryIteratorMode.SYNC;
            // Should be caught by ConfigOption validation
            default -> throw new PermanentBackendException("Unrecognized get-range-mode: " + getRangeModeStr);
        };
        log.info("Using FDB getRange mode: {} (Iterator type: {})", getRangeModeStr, rangeQueryMode);

        maxRetryAttempts = configuration.get(MAX_RETRY_ATTEMPTS);
        retryBackoffMillis = configuration.get(RETRY_BACKOFF_MILLIS);
        log.info("Transaction retry policy: max attempts={}, initial backoff={}ms (for non-serializable isolation)",
            maxRetryAttempts, retryBackoffMillis);

        initializeRootDirectory(rootDirectoryName);

        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .transactional(transactional) // from AbstractStoreManager
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration()) // Use JanusGraph's consistency settings
            .locking(true) // FDB handles concurrency via transactions
            .keyOrdered(true)
            .supportsInterruption(true) // FDB operations are often interruptible
            .optimisticLocking(true) // FDB's core mechanism
            .multiQuery(true)
            .batchMutation(true) // mutateMany groups operations
            .persists(true)
            .build();

        log.info("FoundationDBStoreManager initialized successfully.");
    }

    private void initializeRootDirectory(final String directoryName) throws BackendException {
        try {
            // Use runAsync for potentially long-running directory operations
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(directoryName)).get();
            log.info("Ensured FDB root directory exists: {}", directoryName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TemporaryBackendException("Interrupted while creating/opening FDB root directory", e);
        } catch (Exception e) {
            // Unwrap ExecutionException if present
            Throwable cause = (e instanceof ExecutionException) ? e.getCause() : e;
            throw new PermanentBackendException("Failed to create or open FDB root directory: " + directoryName, cause);
        }
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        // FDB distributes keys automatically, so this concept isn't directly applicable.
        throw new UnsupportedOperationException("FDB does not support local key partitions.");
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {
            // Create a new FDB transaction
            final Transaction fdbRawTx = db.createTransaction();

            final StoreTransaction fdbTx = new FoundationDBTx(db, fdbRawTx, txCfg, isolationLevel, maxRetryAttempts, retryBackoffMillis);

            if (log.isTraceEnabled()) {
                // Avoid creating new Exception object just for logging stack trace
                log.trace("FoundationDB transaction started: {}", fdbTx);
            }

            return fdbTx;
        } catch (FDBException e) {
            throw new PermanentBackendException("Could not start FoundationDB transaction", e);
        } catch (Exception e) {
            // Catch broader exceptions during setup
            throw new PermanentBackendException("Unexpected error starting FoundationDB transaction", e);
        }
    }

    @Override
    public FoundationDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name, "Database name cannot be null");
        // Use computeIfAbsent for atomic and efficient store creation/retrieval
        return stores.computeIfAbsent(name, dbName -> {
            try {
                log.debug("Opening/Creating FDB subspace for store: {}", dbName);
                // Use runAsync for directory operations
                final DirectorySubspace storeDb = rootDirectory.createOrOpen(db, PathUtil.from(dbName)).get();
                log.info("Opened FDB subspace for store: {}", dbName);
                return new FoundationDBKeyValueStore(dbName, storeDb, this);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JanusGraphException("Interrupted while opening FDB subspace: " + dbName, e);
            } catch (Exception e) {
                Throwable cause = (e instanceof ExecutionException) ? e.getCause() : e;
                throw new JanusGraphException("Could not open FDB subspace: " + dbName, cause);
            }
        });
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        FoundationDBTx tx = FoundationDBTx.getTx(txh);
        // No explicit batching needed here as FDB transaction handles atomicity.
        // The loop applies mutations per store within the single transaction.
        for (Map.Entry<String, KVMutation> entry : mutations.entrySet()) {
            String storeName = entry.getKey();
            KVMutation mutation = entry.getValue();
            FoundationDBKeyValueStore store = openDatabase(storeName); // Ensures store subspace exists

            if (!mutation.hasAdditions() && !mutation.hasDeletions()) {
                log.trace("Empty mutation for store {}, skipping.", storeName);
                continue;
            }

            log.debug("Applying mutations to store: {}", storeName);

            // Apply additions
            if (mutation.hasAdditions()) {
                List<KeyValueEntry> additions = mutation.getAdditions();
                log.trace("Applying {} insertions to store {}", additions.size(), storeName);
                for (KeyValueEntry kv : additions) {
                    store.insert(kv.getKey(), kv.getValue(), tx); // Pass the transaction context
                }
            }

            // Apply deletions
            if (mutation.hasDeletions()) {
                List<StaticBuffer> deletions = mutation.getDeletions();
                log.trace("Applying {} deletions to store {}", deletions.size(), storeName);
                for (StaticBuffer key : deletions) {
                    store.delete(key, tx); // Pass the transaction context
                }
            }
        }
        // Commit happens explicitly via txh.commit() later
    }

    // Called by FoundationDBKeyValueStore.close()
    void removeDatabase(FoundationDBKeyValueStore store) {
        Preconditions.checkNotNull(store);
        String name = store.getName();
        if (stores.remove(name) != null) {
            log.debug("Removed reference to closed store: {}", name);
        } else {
            log.warn("Attempted to remove unknown store reference: {}", name);
        }
    }


    @Override
    public void close() throws BackendException {
        log.info("Closing FoundationDBStoreManager...");
        if (!stores.isEmpty()) {
            log.warn("Closing manager while {} stores are still registered: {}", stores.size(), stores.keySet());
            // Consider forcing close on stores or throwing an exception based on desired strictness
            // stores.values().forEach(store -> { try { store.close(); } catch (Exception e) { log.error("Error closing store {}", store.getName(), e); } });
            // stores.clear();
            // For now, just log and proceed.
        }

        if (db != null) {
            try {
                db.close();
            } catch (Exception e) {
                throw new PermanentBackendException("Could not close FoundationDB database", e);
            }
        }

        log.info("FoundationDBStoreManager closed");
    }

    @Override
    public void clearStorage() throws BackendException {
        log.warn("Clearing ALL data within the root directory: {}", rootDirectoryName);
        Preconditions.checkState(rootDirectory != null, "Root directory is not initialized.");
        try {
            // Remove the entire root directory subspace
            DirectoryLayer.getDefault().remove(db, PathUtil.from(rootDirectoryName)).get();
            log.info("Cleared storage by removing directory: {}", rootDirectoryName);
            // Re-initialize the directory after clearing
            initializeRootDirectory(rootDirectoryName);
            stores.clear(); // Clear cached store instances
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TemporaryBackendException("Interrupted while clearing FDB storage", e);
        } catch (Exception e) {
            Throwable cause = (e instanceof ExecutionException) ? e.getCause() : e;
            throw new PermanentBackendException("Could not clear FDB storage (directory: " + rootDirectoryName + ")", cause);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        Preconditions.checkState(db != null, "Database connection is not initialized.");
        Preconditions.checkNotNull(rootDirectoryName, "Root directory name is not configured.");
        try {
            // Check if the root directory subspace exists
            Boolean exists = DirectoryLayer.getDefault().exists(db, PathUtil.from(rootDirectoryName)).get();
            log.debug("Checking existence of root directory '{}': {}", rootDirectoryName, exists);
            return exists != null && exists;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TemporaryBackendException("Interrupted while checking FDB directory existence", e);
        } catch (Exception e) {
            Throwable cause = (e instanceof ExecutionException) ? e.getCause() : e;
            throw new PermanentBackendException("Could not check FDB directory existence: " + rootDirectoryName, cause);
        }
    }

    @Override
    public String getName() {
        // Use the configured directory name or a fixed identifier
        return "FoundationDB[" + rootDirectoryName + "]";
    }

    // Helper to get directory name, prioritizing explicit config over graph name
    private String determineRootDirectoryName(Configuration config) {
        if (config.has(DIRECTORY)) {
            return config.get(DIRECTORY);
        } else if (config.has(GRAPH_NAME)) {
            log.warn("FoundationDB directory not explicitly configured ({}). Using graph name '{}' as directory.",
                DIRECTORY.getName(), GRAPH_NAME.getName());
            return config.get(GRAPH_NAME);
        } else {
            // Fallback to the default value of the DIRECTORY option
            String defaultDir = DIRECTORY.getDefaultValue();
            log.warn("FoundationDB directory not explicitly configured ({}). Using default directory name '{}'.",
                DIRECTORY.getName(), defaultDir);
            return defaultDir;
        }
    }

    // Expose the iterator mode for KeyValueStore
    public RangeQueryIteratorMode getRangeQueryMode() {
        return rangeQueryMode;
    }
}
