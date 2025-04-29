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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;

import java.util.Objects;

/**
 * Helper class to encapsulate FoundationDB specific query parameters
 * derived from a JanusGraph {@link KVQuery}.
 *
 * @author Florian Grieskamp
 * @author JanusGraph Authors
 */
public class FoundationDBRangeQuery {

    private final KVQuery originalQuery;
    private final KeySelector startKeySelector;
    private final KeySelector endKeySelector;
    private final int limit;
    private final Subspace subspace; // Keep subspace for context

    public FoundationDBRangeQuery(Subspace subspace, KVQuery kvQuery) {
        this.subspace = Objects.requireNonNull(subspace);
        this.originalQuery = Objects.requireNonNull(kvQuery);
        this.limit = kvQuery.getLimit();

        final StaticBuffer keyStart = kvQuery.getStart();
        final StaticBuffer keyEnd = kvQuery.getEnd();

        // Pack keys within the store's subspace
        byte[] startKeyBytes = (keyStart == null || keyStart.length() == 0) ?
            subspace.range().begin : subspace.pack(keyStart.as(FoundationDBKeyValueStore.ENTRY_FACTORY));

        byte[] endKeyBytes = (keyEnd == null || keyEnd.length() == 0) ?
            subspace.range().end : subspace.pack(keyEnd.as(FoundationDBKeyValueStore.ENTRY_FACTORY));

        // Standard selectors: firstGreaterOrEqual for start, firstGreaterOrEqual for end (exclusive)
        // FDB ranges are [start, end)
        this.startKeySelector = KeySelector.firstGreaterOrEqual(startKeyBytes);
        this.endKeySelector = KeySelector.firstGreaterOrEqual(endKeyBytes); // Usually corresponds to the start of the *next* key
    }

    public KVQuery asKVQuery() { return originalQuery; }

    public KeySelector getStartKeySelector() { return startKeySelector; }

    public KeySelector getEndKeySelector() { return endKeySelector; }

    public int getLimit() { return limit; }
}
