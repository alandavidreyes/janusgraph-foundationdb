// Copyright 2020 JanusGraph Authors
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
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A RecordIterator wrapping a standard Java Iterator over FoundationDB KeyValue pairs,
 * typically used for synchronous (in-memory list) results. Applies filtering via KeySelector.
 */
public class FoundationDBRecordIterator implements RecordIterator<KeyValueEntry> {
    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordIterator.class);

    protected final Subspace subspace; // Used for unpacking keys
    protected final Iterator<KeyValue> entries;
    protected final KeySelector selector;

    // State for lazy fetching and filtering
    protected KeyValueEntry nextKeyValueEntry;
    protected boolean initialNextCalled = false; // Tracks if hasNext logic has run at least once

    public FoundationDBRecordIterator(Subspace subspace, Iterator<KeyValue> keyValues, KeySelector selector) {
        this.subspace = Objects.requireNonNull(subspace);
        this.entries = Objects.requireNonNull(keyValues);
        this.selector = Objects.requireNonNull(selector);
        this.nextKeyValueEntry = null; // Start with no entry prefetched
        log.trace("FoundationDBRecordIterator created.");
    }

    @Override
    public boolean hasNext() {
        // Fetch the next valid entry if we haven't already or if the last one was consumed
        if (nextKeyValueEntry == null) {
            fetchNextMatchingEntry();
        }
        return nextKeyValueEntry != null;
    }

    @Override
    public KeyValueEntry next() {
        if (!hasNext()) { // Ensures fetchNextMatchingEntry has been called
            throw new NoSuchElementException();
        }
        // Consume the prefetched entry
        KeyValueEntry result = nextKeyValueEntry;
        nextKeyValueEntry = null; // Reset so hasNext fetches the *next* one
        return result;
    }

    /**
     * Advances the underlying iterator until an entry matching the KeySelector is found,
     * or the iterator is exhausted. Stores the matching entry in nextKeyValueEntry.
     */
    protected void fetchNextMatchingEntry() {
        log.trace("Fetching next matching entry...");
        while (entries.hasNext()) {
            KeyValue kv = entries.next();
            byte[] rawKey = kv.getKey();
            StaticBuffer key;
            try {
                // Unpack the key relative to the store's subspace
                key = FoundationDBKeyValueStore.getBuffer(subspace.unpack(rawKey).getBytes(0));
            } catch (IllegalArgumentException e) {
                log.warn("Failed to unpack key '{}' from subspace '{}'. Skipping.", rawKey, subspace.getKey(), e);
                continue; // Skip keys that don't belong to the subspace
            }

            if (selector.include(key)) {
                // Found a matching entry
                nextKeyValueEntry = new KeyValueEntry(key, FoundationDBKeyValueStore.getBuffer(kv.getValue()));
                log.trace("Found matching entry for key: {}", key);
                return; // Exit the loop, entry is stored
            } else {
                log.trace("Key {} excluded by selector.", key);
            }
        }
        // If loop finishes without finding an entry, nextKeyValueEntry remains null
        log.trace("No more matching entries found.");
    }


    @Override
    public void close() {
        // No specific resources to close for a standard Iterator wrapper
        log.trace("FoundationDBRecordIterator closed.");
        // If the underlying iterator implemented Closeable, we'd call it here.
    }

    @Override
    public void remove() {
        // Read-only iterator
        throw new UnsupportedOperationException("Remove operation is not supported.");
    }
}
