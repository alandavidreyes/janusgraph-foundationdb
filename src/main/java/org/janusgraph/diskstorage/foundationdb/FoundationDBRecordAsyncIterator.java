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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.subspace.Subspace;

import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletionException;


/**
 * A RecordIterator wrapping a FoundationDB AsyncIterator.
 * Handles asynchronous fetching and applies filtering via KeySelector.
 * NOTE: This iterator does NOT perform transaction restarts internally.
 * Transaction-level errors during iteration will propagate up and should be
 * handled by retrying the entire operation (e.g., the getSlice call).
 */
public class FoundationDBRecordAsyncIterator implements RecordIterator<KeyValueEntry> {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBRecordAsyncIterator.class);

    private final Subspace subspace;
    private final FoundationDBTx txContext; // Keep for context/logging if needed
    private final FoundationDBRangeQuery originatingQuery; // For context/logging
    private final AsyncIterator<KeyValue> fdbIterator;
    private final KeySelector selector;

    // State for async fetching
    private KeyValueEntry nextKeyValueEntry; // The prefetched entry
    private boolean hasFetched = false;      // Whether we have tried fetching the *next* element
    private boolean finished = false;        // If the underlying iterator is exhausted
    private boolean closed = false;          // If close() has been called

    public FoundationDBRecordAsyncIterator(
        Subspace ds,
        FoundationDBTx tx, // Pass for context, not for restarting
        FoundationDBRangeQuery query,
        AsyncIterator<KeyValue> result,
        KeySelector selector)
    {
        this.subspace = Objects.requireNonNull(ds);
        this.txContext = Objects.requireNonNull(tx); // Keep tx reference for context
        this.originatingQuery = Objects.requireNonNull(query);
        this.fdbIterator = Objects.requireNonNull(result);
        this.selector = Objects.requireNonNull(selector);
        this.nextKeyValueEntry = null;
        log.trace("FoundationDBRecordAsyncIterator created for tx {}", tx.toString()); // Use tx.toString() safely
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            log.warn("hasNext() called on closed iterator for tx {}", txContext);
            return false;
        }
        if (nextKeyValueEntry == null && !finished && !hasFetched) {
            try {
                fetchNextMatchingEntry();
                hasFetched = true; // Mark that we attempted a fetch
            } catch (Exception e) {
                // Log and wrap unexpected errors during hasNext fetch
                log.error("Error fetching next element in hasNext() for tx {}", txContext, e);
                close(); // Close the iterator on error
                throw new RuntimeException("Failed to fetch next element from FDB", e);
            }
        }
        return nextKeyValueEntry != null;
    }

    @Override
    public KeyValueEntry next() {
        if (closed) {
            throw new NoSuchElementException("Iterator has been closed.");
        }
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements available.");
        }

        KeyValueEntry result = nextKeyValueEntry;
        // Reset state for the *next* call to hasNext()
        nextKeyValueEntry = null;
        hasFetched = false;
        return result;
    }

    /**
     * Asynchronously fetches the next KeyValue from the FDB iterator,
     * applies the KeySelector, and stores the result in nextKeyValueEntry
     * if it matches. Updates the 'finished' flag.
     *
     * This method blocks until the next matching item is found or the iterator ends.
     */
    private void fetchNextMatchingEntry() {
        log.trace("Async fetching next matching entry for tx {}", txContext);
        while (!finished) {
            try {
                // Block and wait for the next item from FDB
                Boolean hasMore = fdbIterator.onHasNext().join(); // join can throw CompletionException

                if (hasMore != null && hasMore) {
                    KeyValue kv = fdbIterator.next();
                    byte[] rawKey = kv.getKey();
                    StaticBuffer key;

                    try {
                        key = FoundationDBKeyValueStore.getBuffer(subspace.unpack(rawKey).getBytes(0));
                    } catch (IllegalArgumentException e) {
                        log.warn("Tx {} - Failed to unpack key '{}' from subspace '{}'. Skipping.", txContext, rawKey, subspace.getKey(), e);
                        continue; // Skip and try the next item
                    }

                    if (selector.include(key)) {
                        nextKeyValueEntry = new KeyValueEntry(key, FoundationDBKeyValueStore.getBuffer(kv.getValue()));
                        log.trace("Tx {} - Found matching async entry for key: {}", txContext, key);
                        return; // Found a match, exit the loop
                    } else {
                        log.trace("Tx {} - Async key {} excluded by selector.", txContext, key);
                        // Continue loop to fetch the next item
                    }
                } else {
                    // Underlying iterator is exhausted
                    log.trace("Tx {} - Underlying FDB async iterator finished.", txContext);
                    finished = true;
                    nextKeyValueEntry = null; // Ensure no dangling entry
                    return; // Exit loop
                }
            } catch (CompletionException e) {
                // Handle exceptions from the CompletableFuture.join()
                Throwable cause = e.getCause();
                log.error("Tx {} - FDBException or other error during async iteration.", txContext, cause);

                // Close the iterator on error
                close();

                // Rethrow wrapped exception
                if (cause instanceof FDBException fdbEx) {
                    // Let the caller handle FDBException (e.g., by retrying the whole operation)
                    // Do NOT attempt tx.restart() here.
                    throw fdbEx; // Re-throw FDBException directly
                } else {
                    // Wrap unexpected errors
                    throw new RuntimeException("Unexpected error during FDB async iteration", cause);
                }
            } catch (Exception e) {
                // Catch any other synchronous errors during processing
                log.error("Tx {} - Unexpected synchronous error during async iteration.", txContext, e);
                close();
                throw new RuntimeException("Unexpected synchronous error processing FDB async result", e);
            }
        } // End while loop

        // If loop finishes (because finished = true), ensure next is null
        nextKeyValueEntry = null;
    }


    @Override
    public void close() {
        if (!closed) {
            closed = true;
            log.debug("Closing FoundationDBRecordAsyncIterator for tx {}", txContext);
            try {
                // Cancel the underlying FDB AsyncIterator to release resources
                fdbIterator.cancel();
            } catch (Exception e) {
                // Log error but don't prevent closing state change
                log.warn("Tx {} - Error cancelling FDB AsyncIterator during close.", txContext, e);
            }
            nextKeyValueEntry = null; // Clear any prefetched entry
        }
    }

    @Override
    public void remove() {
        // FDB iterators are typically read-only
        throw new UnsupportedOperationException("Remove operation is not supported.");
    }
}
