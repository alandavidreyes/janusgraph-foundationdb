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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Represents a FoundationDB Transaction.
 * Optimized for concurrency using ReentrantLock instead of synchronized methods.
 * Implements configurable retry logic for non-serializable transactions.
 *
 * @author Ted Wilmes (twilmes@gmail.com)
 * @author JanusGraph Authors (optimizations)
 */
public class FoundationDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(FoundationDBTx.class);

    private final Database db;
    private final IsolationLevel isolationLevel;
    private final int maxRetryAttempts; // Max retries for non-serializable tx
    private final long initialRetryBackoffMillis; // Initial backoff delay

    private volatile Transaction tx; // The current FDB transaction object
    private volatile boolean isOpen; // Flag indicating if the transaction is active

    // Lock to protect access to shared transaction state (tx, inserts, deletions, isOpen)
    private final ReentrantLock txLock = new ReentrantLock();

    // Mutations buffered within this transaction instance
    // Access must be protected by txLock
    private final List<Insert> inserts = new ArrayList<>();
    private final List<byte[]> deletions = new ArrayList<>();

    private final int txId; // For logging/debugging
    private static final AtomicInteger txCounter = new AtomicInteger(0);


    public enum IsolationLevel {SERIALIZABLE, READ_COMMITTED_NO_WRITE, READ_COMMITTED_WITH_WRITE}

    public FoundationDBTx(Database db, Transaction t, BaseTransactionConfig config,
                          IsolationLevel isolationLevel, int maxRetryAttempts, long initialRetryBackoffMillis) {
        super(config);
        this.db = Objects.requireNonNull(db);
        this.tx = Objects.requireNonNull(t);
        this.isolationLevel = Objects.requireNonNull(isolationLevel);
        // Only apply retries for non-serializable levels
        this.maxRetryAttempts = (isolationLevel == IsolationLevel.SERIALIZABLE) ? 0 : maxRetryAttempts;
        this.initialRetryBackoffMillis = initialRetryBackoffMillis;
        this.isOpen = true;
        this.txId = txCounter.incrementAndGet();
        log.trace("Tx {} created (Isolation: {}, MaxRetries: {}, InitialBackoff: {}ms)",
            txId, isolationLevel, this.maxRetryAttempts, initialRetryBackoffMillis);
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    // Internal helper to safely close the current transaction
    private void closeCurrentTransaction() {
        if (tx != null) {
            try {
                // Closing cancels if not committed/aborted
                tx.close();
            } catch (FDBException e) {
                log.warn("Tx {} encountered FDBException during close: {} ({})", txId, e.getMessage(), e.getCode(), e);
            } catch (Exception e) {
                log.warn("Tx {} encountered unexpected exception during close: {}", txId, e.getMessage(), e);
            } finally {
                tx = null; // Ensure it's nulled out
            }
        }
    }

    /**
     * Restarts the transaction after a retryable error.
     * Must be called while holding the txLock.
     */
    private void restartTransactionUnsafe() {
        log.debug("Tx {} restarting...", txId);
        closeCurrentTransaction(); // Close the old one first

        tx = db.createTransaction(); // Create a new FDB transaction
        // Configure options on the new transaction if needed
        // tx.options()...

        // Re-apply mutations from this Tx instance
        log.debug("Tx {} re-applying {} inserts and {} deletions after restart.", txId, inserts.size(), deletions.size());
        inserts.forEach(insert -> tx.set(insert.getKey(), insert.getValue()));
        deletions.forEach(delete -> tx.clear(delete));

        // isOpen remains true
    }


    @Override
    public void commit() throws BackendException {
        txLock.lock();
        try {
            if (!isOpen) {
                log.warn("Tx {} commit called on already closed transaction.", txId);
                return; // Or throw? Consistent with rollback.
            }

            super.commit(); // Handles consistency checks

            if (inserts.isEmpty() && deletions.isEmpty()) {
                log.debug("Tx {} has no mutations, cancelling and closing.", txId);
                // No actual work done, just cancel cleanly
                if (tx != null) {
                    tx.cancel();
                }
                isOpen = false;
                closeCurrentTransaction(); // Ensure resources are released
                return;
            }

            log.debug("Tx {} attempting commit with {} inserts, {} deletions.", txId, inserts.size(), deletions.size());

            // **** TYPE HINTING FOR CLARITY (Optional but can help inference) ****
            // Explicitly type the lambda if inference fails, though often not needed.
            // Supplier<CompletableFuture<Void>> commitOperation = () -> { ... };
            // executeWithRetries(commitOperation);
            // For now, rely on improved inference with simpler structure if possible:
            executeWithRetries(() -> { // Lambda for Supplier<CompletableFuture<Void>>
                txLock.lock();
                try {
                    Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during commit retry");
                    return tx.commit(); // Returns CompletableFuture<Void>
                } finally {
                    txLock.unlock();
                }
            });


            log.info("Tx {} committed successfully.", txId);
            isOpen = false;

        } catch (BackendException e) {
            // If executeWithRetries throws (Permanent or final Temporary failure)
            log.error("Tx {} commit failed permanently after retries.", txId, e);
            isOpen = false; // Mark as closed on failure too
            closeCurrentTransaction(); // Clean up FDB resources
            throw e;
        } catch (Throwable t) {
            // Catch any other unexpected errors during commit logic
            log.error("Tx {} commit failed with unexpected error.", txId, t);
            isOpen = false;
            closeCurrentTransaction();
            throw new PermanentBackendException("Unexpected error during commit", t);
        } finally {
            // Ensure lock is always released, even if super.commit() throws
            txLock.unlock();
            // Ensure FDB transaction resources are released if commit succeeded or failed permanently
            if (!isOpen && tx != null) {
                closeCurrentTransaction();
            }
        }
    }


    @Override
    public void rollback() throws BackendException {
        txLock.lock();
        try {
            if (!isOpen) {
                log.warn("Tx {} rollback called on already closed transaction.", txId);
                return;
            }

            super.rollback(); // Mark Transaction as rolled back conceptually

            log.info("Tx {} rolling back.", txId);
            if (tx != null) {
                try {
                    tx.cancel(); // Explicitly cancel
                } catch (FDBException e) {
                    log.warn("Tx {} encountered FDBException during cancel on rollback: {} ({})", txId, e.getMessage(), e.getCode(), e);
                } catch (Exception e) {
                    log.warn("Tx {} encountered unexpected exception during cancel on rollback: {}", txId, e.getMessage(), e);
                }
            }
        } catch (Throwable t) {
            // Catch errors during super.rollback() or tx.cancel()
            log.error("Tx {} error during rollback logic.", txId, t);
            // Still ensure closed state and resource cleanup
            // Throwing PermanentBackendException as rollback failure is serious
            throw new PermanentBackendException("Error during transaction rollback", t);
        } finally {
            isOpen = false; // Mark as closed regardless of cancel success
            closeCurrentTransaction(); // Ensure FDB resources are released
            txLock.unlock();
        }
    }

    // --- Data Access Methods ---

    public byte[] get(final byte[] key) throws BackendException {
        return executeWithRetries(() -> { // Lambda for Supplier<CompletableFuture<byte[]>>
            txLock.lock();
            try {
                Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during get");
                return tx.get(key); // Returns CompletableFuture<byte[]>
            } finally {
                txLock.unlock();
            }
        });
    }

    /**
     * Executes a getRange operation using the sync 'asList()' method.
     * Consider using getRangeIterator for large ranges to avoid high memory usage.
     */
    public List<KeyValue> getRange(final FoundationDBRangeQuery query) throws BackendException {
        return executeWithRetries(() -> { // Lambda for Supplier<CompletableFuture<List<KeyValue>>>
            txLock.lock();
            try {
                Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during getRange");
                ReadTransaction readTx = (isolationLevel == IsolationLevel.SERIALIZABLE) ? tx : tx.snapshot();
                return readTx.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit()).asList();
            } finally {
                txLock.unlock();
            }
        });
    }

    /**
     * Returns an AsyncIterator for a range query. Preferred for large ranges.
     * The caller is responsible for handling potential FDBExceptions during iteration.
     * NOTE: This method does NOT use the retry wrapper, as retries during iteration
     * are complex and often better handled by retrying the entire operation.
     */
    public AsyncIterator<KeyValue> getRangeIterator(FoundationDBRangeQuery query) throws BackendException {
        txLock.lock();
        try {
            Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during getRangeIterator");
            log.trace("Tx {} creating range iterator: start={}, end={}, limit={}", txId,
                query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit());

            ReadTransaction readTx = (isolationLevel == IsolationLevel.SERIALIZABLE) ? tx : tx.snapshot();

            return readTx.getRange(query.getStartKeySelector(), query.getEndKeySelector(), query.getLimit(),
                false, StreamingMode.WANT_ALL).iterator();
        } catch (FDBException e) {
            throw FDBExceptionMapper.map("getRangeIterator setup", e);
        } finally {
            txLock.unlock();
        }
    }

    /**
     * Executes multiple range queries concurrently within the transaction.
     * Uses CompletableFuture for parallelism and the retry wrapper for resilience.
     */
    public Map<KVQuery, List<KeyValue>> getMultiRange(final Collection<FoundationDBRangeQuery> queries) throws BackendException {
        if (queries.isEmpty()) {
            return Collections.emptyMap();
        }

        // **** RESTRUCTURED ASYNC CHAIN for getMultiRange ****
        return executeWithRetries(() -> { // Lambda for Supplier<CompletableFuture<Map<KVQuery, List<KeyValue>>>>

            // 1. Create the map to hold futures, locking only to access 'tx' safely
            final Map<KVQuery, CompletableFuture<List<KeyValue>>> futuresMap = new HashMap<>();
            txLock.lock();
            try {
                Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during getMultiRange");
                ReadTransaction readTx = (isolationLevel == IsolationLevel.SERIALIZABLE) ? tx : tx.snapshot();

                for (FoundationDBRangeQuery fdbQuery : queries) {
                    KVQuery originalQuery = fdbQuery.asKVQuery();
                    CompletableFuture<List<KeyValue>> future = readTx.getRange(
                        fdbQuery.getStartKeySelector(),
                        fdbQuery.getEndKeySelector(),
                        fdbQuery.getLimit()
                    ).asList();
                    futuresMap.put(originalQuery, future);
                }
            } finally {
                txLock.unlock();
            }

            // 2. Get list of futures for allOf
            List<CompletableFuture<?>> futureList = new ArrayList<>(futuresMap.values());

            // 3. Wait for all individual getRange calls to complete (or fail)
            CompletableFuture<Void> allDone = CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[0]));

            // 4. When allDone completes, *then* process the results. Run this processing step async.
            CompletableFuture<Map<KVQuery, List<KeyValue>>> resultProcessingFuture = allDone.thenApplyAsync(v -> {
                Map<KVQuery, List<KeyValue>> resultMap = new ConcurrentHashMap<>(); // Use concurrent map for safety
                futuresMap.forEach((query, future) -> {
                    try {
                        // .join() is safe here because allDone ensured completion.
                        // It will throw CompletionException if the *individual* future failed.
                        List<KeyValue> result = future.join();
                        resultMap.put(query, result != null ? result : Collections.emptyList());
                    } catch (CompletionException ce) {
                        // IMPORTANT: If an individual future failed, we need to propagate
                        // the cause so executeWithRetries can potentially retry.
                        // Wrap the original cause in a CompletionException to signal failure upwards.
                        log.debug("Tx {} - Individual getRange failed within getMultiRange", txId, ce.getCause());
                        throw ce; // Re-throw CompletionException containing the original cause
                    }
                });
                return resultMap; // Return the fully populated map
            }, FDB.DEFAULT_EXECUTOR); // Use FDB's executor for the result processing

            // The executeWithRetries function needs the CompletableFuture holding the final result.
            return resultProcessingFuture;

        }); // End executeWithRetries call
    }


    // --- Mutation Methods ---

    public void set(final byte[] key, final byte[] value) throws PermanentBackendException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        txLock.lock();
        try {
            Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during set");
            inserts.add(new Insert(key, value));
            tx.set(key, value);
        } catch (FDBException e) {
            throw new PermanentBackendException("FDB error during set operation", e);
        } finally {
            txLock.unlock();
        }
    }

    public void clear(final byte[] key) throws PermanentBackendException {
        Objects.requireNonNull(key);
        txLock.lock();
        try {
            Preconditions.checkState(isOpen && tx != null, "Transaction state invalid during clear");
            deletions.add(key);
            tx.clear(key);
        } catch (FDBException e) {
            throw new PermanentBackendException("FDB error during clear operation", e);
        } finally {
            txLock.unlock();
        }
    }

    // --- Retry Logic ---

    /**
     * Executes a transactional FDB operation with retry logic for transient errors.
     *
     * @param operation A function that takes no arguments and returns a CompletableFuture
     *                  representing the asynchronous FDB operation. The function may be
     *                  called multiple times if retries occur. It *must* re-acquire the txLock
     *                  if needed to access shared state like 'tx'.
     * @param <T> The result type of the CompletableFuture.
     * @return The result of the operation (obtained by blocking on the future).
     * @throws BackendException if the operation fails permanently or after exhausting retries.
     */
    private <T> T executeWithRetries(Supplier<CompletableFuture<T>> operation) throws BackendException {
        long currentBackoffMillis = initialRetryBackoffMillis;
        Exception lastException = null; // Store last exception (FDB or other)

        for (int attempt = 0; attempt <= maxRetryAttempts; attempt++) {
            try {
                // Execute the operation supplier to get the Future, then block for the result.
                T result = operation.get().join(); // join() throws CompletionException on async failure

                if (attempt > 0) {
                    log.info("Tx {} operation succeeded after {} retries.", txId, attempt);
                }
                return result; // Success

            } catch (CompletionException e) {
                // Handle exceptions thrown by CompletableFuture.join()
                Throwable cause = e.getCause();
                lastException = e; // Store the CompletionException itself or its cause

                if (cause instanceof FDBException fdbEx) {
                    lastException = fdbEx; // Prefer the FDBException
                    logFDBException(fdbEx, attempt);

                    if (fdbEx.isRetryable() && attempt < maxRetryAttempts) {
                        // Perform retry logic
                        if (retryCommit(fdbEx, attempt, currentBackoffMillis)) {
                            currentBackoffMillis *= 2; // Exponential backoff
                            continue; // Continue to next iteration
                        } else {
                            // retryCommit decided not to retry (e.g., transaction closed)
                            throw FDBExceptionMapper.map("FDB operation aborted during retry", fdbEx);
                        }
                    } else {
                        // Non-retryable FDB error or max retries exceeded
                        log.error("Tx {} failed with non-retryable FDB error (Code {}) or max retries ({}) exceeded.",
                            txId, fdbEx.getCode(), maxRetryAttempts, fdbEx);
                        throw FDBExceptionMapper.map("FDB operation failed permanently", fdbEx);
                    }
                } else {
                    // Cause is not an FDBException
                    log.error("Tx {} failed with unexpected error within CompletableFuture.", txId, cause);
                    throw new PermanentBackendException("Unexpected error during FDB async operation", cause);
                }
            } catch (FDBException fdbEx) {
                // Catch FDBException directly if thrown synchronously (less common with async setup)
                lastException = fdbEx;
                logFDBException(fdbEx, attempt);
                if (fdbEx.isRetryable() && attempt < maxRetryAttempts) {
                    // Perform retry logic
                    if (retryCommit(fdbEx, attempt, currentBackoffMillis)) {
                        currentBackoffMillis *= 2;
                        continue;
                    } else {
                        throw FDBExceptionMapper.map("FDB operation aborted during sync retry", fdbEx);
                    }
                } else {
                    log.error("Tx {} failed with non-retryable sync FDB error (Code {}) or max retries ({}) exceeded.",
                        txId, fdbEx.getCode(), maxRetryAttempts, fdbEx);
                    throw FDBExceptionMapper.map("FDB sync operation failed permanently", fdbEx);
                }
            } catch (Throwable t) {
                // Catch any other unexpected synchronous errors from operation.get() itself
                lastException = (Exception) t; // Cast needed? Assume Exception for now
                log.error("Tx {} failed with unexpected synchronous error.", txId, t);
                throw new PermanentBackendException("Unexpected synchronous error during FDB operation", t);
            }
        } // End retry loop

        // Only reached if max retries were exceeded without returning/throwing earlier
        log.error("Tx {} operation failed after exceeding max retry attempts ({}). Last error: {}",
            txId, maxRetryAttempts, lastException != null ? lastException.getMessage() : "N/A", lastException);
        // Throw TemporaryBackendException as the *last* failure was likely retryable but hit limit
        throw new TemporaryBackendException("Operation failed after " + maxRetryAttempts + " retries.", lastException);
    }

    /**
     * Handles the logic for retrying a commit/operation after a retryable FDBException.
     * Acquires lock, checks state, performs backoff, and restarts the transaction.
     *
     * @return true if retry should proceed, false otherwise (e.g., tx closed).
     * @throws TemporaryBackendException if interrupted during backoff.
     * @throws PermanentBackendException if transaction closed during retry attempt.
     */
    private boolean retryCommit(FDBException fdbEx, int attempt, long currentBackoffMillis) throws BackendException {
        txLock.lock();
        try {
            if (!isOpen) { // Check if closed by another thread concurrently
                log.warn("Tx {} was closed during retry attempt {}. Aborting retry.", txId, attempt);
                // Don't throw here, let executeWithRetries handle the final FDBException as Permanent
                return false;
            }
            log.warn("Tx {} encountered retryable FDB error (code {}): {}. Attempt {}/{}. Retrying after {}ms...",
                txId, fdbEx.getCode(), fdbEx.getMessage(), attempt, maxRetryAttempts, currentBackoffMillis);

            // Backoff before restarting and retrying
            try {
                Thread.sleep(currentBackoffMillis);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new TemporaryBackendException("Interrupted during retry backoff", ie);
            }

            restartTransactionUnsafe(); // Restart FDB tx and reapply mutations
            return true; // Signal that retry should continue

        } finally {
            txLock.unlock();
        }
    }


    private void logFDBException(FDBException fe, int attempt) {
        // Guard logging call
        if (log.isDebugEnabled()) {
            log.debug("Tx {} - FDBException Details (Attempt {}): Code={}, Message='{}', Retryable={}, MaybeCommitted={}, RetryableNotCommitted={}, Success={}",
                txId, attempt, fe.getCode(), fe.getMessage(), fe.isRetryable(),
                fe.isMaybeCommitted(), fe.isRetryableNotCommitted(), fe.isSuccess());
        }
    }

    // --- Helper & Internal Classes ---

    /** Simple holder for key-value pairs to be inserted. */
    private static class Insert {
        private final byte[] key;
        private final byte[] value;

        Insert(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
        byte[] getKey() { return key; }
        byte[] getValue() { return value; }
    }

    /** Utility to map FDBExceptions to JanusGraph BackendExceptions. */
    private static class FDBExceptionMapper {
        static BackendException map(String context, FDBException e) {
            if (e.isRetryable()) {
                // Even if FDB says retryable, if we exhausted retries, it becomes Temporary from JG's perspective
                return new TemporaryBackendException(STR."\{context}: \{e.getMessage()}", e);
            } else {
                return new PermanentBackendException(STR."\{context}: \{e.getMessage()}", e);
            }
        }
    }

    // Static helper to safely cast StoreTransaction
    static FoundationDBTx getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh instanceof FoundationDBTx,
            "Expected FoundationDBTx, but got %s", txh != null ? txh.getClass().getName() : "null");
        return (FoundationDBTx) txh;
    }

    @Override
    public String toString() {
        return STR."FoundationDBTx[id=\{txId}, isolation=\{isolationLevel}, isOpen=\{isOpen}]";
    }
}
