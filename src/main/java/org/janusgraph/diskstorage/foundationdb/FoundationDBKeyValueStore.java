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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * FoundationDB implementation of {@link OrderedKeyValueStore}.
 * Delegates operations to {@link FoundationDBTx}.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 * @author JanusGraph Authors
 */
public class FoundationDBKeyValueStore implements OrderedKeyValueStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBKeyValueStore.class);

    static final StaticBuffer.Factory<byte[]> ENTRY_FACTORY = Arrays::copyOfRange;

    private final DirectorySubspace dbSubspace; // Represents the specific FDB subspace for this store
    private final String name;
    private final FoundationDBStoreManager manager;
    private volatile boolean isOpen;

    public FoundationDBKeyValueStore(String name, DirectorySubspace subspace, FoundationDBStoreManager manager) {
        this.dbSubspace = Preconditions.checkNotNull(subspace);
        this.name = Preconditions.checkNotNull(name);
        this.manager = Preconditions.checkNotNull(manager);
        this.isOpen = true;
        log.debug("FoundationDBKeyValueStore '{}' initialized with subspace prefix: {}", name, subspace.getKey());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized void close() throws BackendException {
        // Use synchronized for simple state check/update, low contention expected here.
        if (isOpen) {
            log.info("Closing FoundationDBKeyValueStore: {}", name);
            isOpen = false;
            manager.removeDatabase(this); // Notify manager
            // dbSubspace doesn't need explicit closing, FDB connection is managed by StoreManager
        } else {
            log.warn("FoundationDBKeyValueStore '{}' already closed.", name);
        }
    }

    private void ensureOpen() throws PermanentBackendException {
        if (!isOpen) {
            throw new PermanentBackendException("Store '" + name + "' has been closed.");
        }
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        ensureOpen();
        FoundationDBTx tx = FoundationDBTx.getTx(txh);
        byte[] databaseKey = dbSubspace.pack(key.as(ENTRY_FACTORY));
        log.trace("Store '{}', op=get, key={}, tx={}", name, key, tx);

        byte[] valueBytes = tx.get(databaseKey);

        return (valueBytes != null) ? getBuffer(valueBytes) : null;
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        // FoundationDB uses Optimistic Concurrency Control with transaction retries.
        // JanusGraph's explicit locking via acquireLock is generally NOT needed or recommended
        // when using FDB's serializable isolation. Conflicts are detected at commit time.
        // If using weaker isolation, application-level locking might be needed, but that
        // pattern is discouraged with FDB.
        // For compatibility, we don't throw, but log a warning.
        if (FoundationDBTx.getTx(txh).getIsolationLevel() == FoundationDBTx.IsolationLevel.SERIALIZABLE) {
            log.debug("Store '{}', op=acquireLock (ignored - using FDB optimistic locking), key={}, tx={}", name, key, txh);
        } else {
            log.warn("Store '{}', op=acquireLock called with non-serializable isolation level. " +
                "FDB relies on transaction conflicts, explicit locking may be ineffective or lead to deadlocks. Key={}, tx={}", name, key, txh);
        }
        // No operation needed with FDB's model.
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        ensureOpen();
        FoundationDBTx tx = FoundationDBTx.getTx(txh);
        var rangeQuery = new FoundationDBRangeQuery(dbSubspace, query);

        // Delegate based on configured mode in the manager
        if (manager.getRangeQueryMode() == FoundationDBStoreManager.RangeQueryIteratorMode.ASYNC) {
            return getSliceAsync(rangeQuery, tx);
        } else {
            return getSliceSync(rangeQuery, tx);
        }
    }

    private RecordIterator<KeyValueEntry> getSliceSync(FoundationDBRangeQuery rangeQuery, FoundationDBTx tx) throws BackendException {
        KVQuery query = rangeQuery.asKVQuery();
        log.trace("Store '{}', op=getSliceSync, query={}, tx={}", name, query, tx);

        List<KeyValue> resultList = tx.getRange(rangeQuery); // Uses retry logic internally

        log.debug("Store '{}', op=getSliceSync, query={}, tx={}, resultCount={}", name, query, tx, resultList.size());

        // Wrap the in-memory list with a standard iterator
        return new FoundationDBRecordIterator(dbSubspace, resultList.iterator(), query.getKeySelector());
    }

    private RecordIterator<KeyValueEntry> getSliceAsync(FoundationDBRangeQuery rangeQuery, FoundationDBTx tx) throws BackendException {
        KVQuery query = rangeQuery.asKVQuery();
        log.trace("Store '{}', op=getSliceAsync, query={}, tx={}", name, query, tx);

        try {
            // Get the FDB async iterator (does not use retry logic itself)
            AsyncIterator<KeyValue> fdbIterator = tx.getRangeIterator(rangeQuery);

            // Wrap FDB's async iterator
            // Pass the *transaction* context in case it's needed (though removed retry logic from iterator itself)
            return new FoundationDBRecordAsyncIterator(dbSubspace, tx, rangeQuery, fdbIterator, query.getKeySelector());
        } catch (BackendException e) {
            log.error("Store '{}', op=getSliceAsync failed during iterator setup. Query={}, tx={}", name, query, tx, e);
            throw e; // Re-throw known BackendExceptions
        } catch (Exception e) {
            log.error("Store '{}', op=getSliceAsync failed with unexpected error during iterator setup. Query={}, tx={}", name, query, tx, e);
            throw new PermanentBackendException("Failed to initialize async slice iterator", e);
        }
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        ensureOpen();
        FoundationDBTx tx = FoundationDBTx.getTx(txh);

        if (manager.getRangeQueryMode() == FoundationDBStoreManager.RangeQueryIteratorMode.ASYNC) {
            return getSlicesAsync(queries, tx);
        } else {
            return getSlicesSync(queries, tx);
        }
    }


    private Map<KVQuery, RecordIterator<KeyValueEntry>> getSlicesSync(List<KVQuery> queries, FoundationDBTx tx) throws BackendException {
        log.trace("Store '{}', op=getSlicesSync, queries={}, tx={}", name, queries.size(), tx);
        Map<KVQuery, FoundationDBRangeQuery> fdbQueries = queries.stream()
            .collect(Collectors.toMap(q -> q, q -> new FoundationDBRangeQuery(dbSubspace, q)));

        Map<KVQuery, List<KeyValue>> resultMap = tx.getMultiRange(fdbQueries.values()); // Uses retry logic

        Map<KVQuery, RecordIterator<KeyValueEntry>> iteratorMap = new HashMap<>(resultMap.size());
        resultMap.forEach((query, kvList) -> {
            iteratorMap.put(query, new FoundationDBRecordIterator(dbSubspace, kvList.iterator(), query.getKeySelector()));
        });

        log.debug("Store '{}', op=getSlicesSync, queries={}, tx={}, resultSets={}", name, queries.size(), tx, iteratorMap.size());
        return iteratorMap;
    }

    private Map<KVQuery, RecordIterator<KeyValueEntry>> getSlicesAsync(List<KVQuery> queries, FoundationDBTx tx) throws BackendException {
        log.trace("Store '{}', op=getSlicesAsync, queries={}, tx={}", name, queries.size(), tx);
        Map<KVQuery, RecordIterator<KeyValueEntry>> resultMap = new HashMap<>(queries.size());

        try {
            for (final KVQuery query : queries) {
                FoundationDBRangeQuery rangeQuery = new FoundationDBRangeQuery(dbSubspace, query);
                // Get iterator, handle exceptions during setup
                AsyncIterator<KeyValue> fdbIterator = tx.getRangeIterator(rangeQuery);
                resultMap.put(query, new FoundationDBRecordAsyncIterator(dbSubspace, tx, rangeQuery, fdbIterator, query.getKeySelector()));
            }
        } catch (BackendException e) {
            log.error("Store '{}', op=getSlicesAsync failed during iterator setup for one or more queries. Tx={}", name, tx, e);
            // Clean up any iterators already created? Difficult. Re-throw.
            resultMap.values().forEach(iter -> { try { iter.close(); } catch (Exception ignored) {} });
            throw e;
        } catch (Exception e) {
            log.error("Store '{}', op=getSlicesAsync failed with unexpected error during setup. Tx={}", name, tx, e);
            resultMap.values().forEach(iter -> { try { iter.close(); } catch (Exception ignored) {} });
            throw new PermanentBackendException("Failed to initialize one or more async slice iterators", e);
        }

        log.debug("Store '{}', op=getSlicesAsync, queries={}, tx={}, iteratorsCreated={}", name, queries.size(), tx, resultMap.size());
        return resultMap;
    }


    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        // FoundationDB doesn't directly support TTLs at the storage layer.
        // TTLs in JanusGraph are typically handled by background jobs checking timestamps.
        if (ttl != null && ttl > 0) {
            log.warn("Store '{}', op=insert with TTL={} requested, but FoundationDB backend does not natively support TTL. TTL ignored. Key={}", name, ttl, key);
        }
        insert(key, value, txh); // Call the non-TTL version
    }

    // Internal insert implementation called by mutateMany and public insert variants
    void insert(StaticBuffer key, StaticBuffer value, FoundationDBTx tx) throws BackendException {
        ensureOpen();
        byte[] dbKey = dbSubspace.pack(key.as(ENTRY_FACTORY));
        byte[] dbValue = value.as(ENTRY_FACTORY);
        log.trace("Store '{}', op=insert, key={}, valueSize={}, tx={}", name, key, dbValue.length, tx);
        tx.set(dbKey, dbValue);
    }

    // Public interface version
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        insert(key, value, FoundationDBTx.getTx(txh));
    }

    // Internal delete implementation called by mutateMany
    void delete(StaticBuffer key, FoundationDBTx tx) throws BackendException {
        ensureOpen();
        byte[] dbKey = dbSubspace.pack(key.as(ENTRY_FACTORY));
        log.trace("Store '{}', op=delete, key={}, tx={}", name, key, tx);
        tx.clear(dbKey);
    }

    // Public interface version
    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        delete(key, FoundationDBTx.getTx(txh));
    }

    // Helper to convert byte[] to StaticBuffer using efficient buffer pooling (StaticArrayBuffer does this)
    static StaticBuffer getBuffer(byte[] entry) {
        return StaticArrayBuffer.of(entry);
    }
}
