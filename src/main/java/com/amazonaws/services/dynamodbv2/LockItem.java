/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2;

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A lock acquired via {@link AmazonDynamoDBLockClient}. Extends {@link LockItemInfo} with
 * sync client operations: {@link #close()}, {@link #sendHeartBeat()}, and {@link #ensure}.
 * Implements {@link Closeable} so it can be used in try-with-resources blocks.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class LockItem extends LockItemInfo implements Closeable {

    private final AmazonDynamoDBLockClient client;

    /**
     * Creates a lock item representing a given key. This constructor should
     * only be called by the lock client -- the caller should use the version of
     * LockItem returned by {@code acquireLock}. In order to enforce this, it is
     * package-private.
     *
     * @param client                        The AmazonDynamoDBLockClient object associated with this lock
     * @param partitionKey                  The key representing the lock
     * @param sortKey                       The sort key, if the DynamoDB table supports sort keys (or
     *                                      Optional.absent otherwise)
     * @param data                          The data stored in the lock (can be null)
     * @param deleteLockItemOnClose         Whether or not to delete the lock item when releasing it
     * @param ownerName                     The owner associated with the lock
     * @param leaseDuration                 How long the lease for the lock is (in milliseconds)
     * @param lastUpdatedTimeInMilliseconds How recently the lock was updated (in milliseconds)
     * @param recordVersionNumber           The current record version number of the lock -- this is
     *                                      globally unique and changes each time the lock is updated
     * @param isReleased                    Whether the item in DynamoDB is marked as released, but still
     *                                      exists in the table
     * @param sessionMonitor                Optionally, the SessionMonitor object with which to associate
     *                                      the lock
     * @param additionalAttributes          Additional attributes that can optionally be stored alongside
     *                                      the lock
     */
    LockItem(final AmazonDynamoDBLockClient client, final String partitionKey, final Optional<String> sortKey,
            final Optional<ByteBuffer> data, final boolean deleteLockItemOnClose, final String ownerName,
            final long leaseDuration, final long lastUpdatedTimeInMilliseconds, final String recordVersionNumber,
            final boolean isReleased, final Optional<SessionMonitor> sessionMonitor,
            final Map<String, AttributeValue> additionalAttributes) {
        super(partitionKey, sortKey, data, deleteLockItemOnClose, ownerName, leaseDuration,
                lastUpdatedTimeInMilliseconds, recordVersionNumber, isReleased, sessionMonitor,
                additionalAttributes);
        Objects.requireNonNull(client, "Cannot create a lock with a null client");
        this.client = client;
    }

    /**
     * Releases the lock for others to use.
     */
    @Override
    public void close() {
        this.client.releaseLock(this);
    }

    /**
     * Returns a string representation of this lock.
     */
    @Override
    public String toString() {
        String dataString = getData()
            .map(byteBuffer -> new String(byteBuffer.array(), StandardCharsets.UTF_8))
            .orElse("");
        return String
            .format("LockItem{Partition Key=%s, Sort Key=%s, Owner Name=%s, Lookup Time=%d, Lease Duration=%d, "
                    + "Record Version Number=%s, Delete On Close=%s, Data=%s, Is Released=%s}",
                getPartitionKey(), getSortKey(), getOwnerName(), getLookupTime(), getLeaseDuration(),
                getRecordVersionNumber(), getDeleteLockItemOnClose(), dataString, isReleased());
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || !(other instanceof LockItemInfo)) {
            return false;
        }
        final LockItemInfo o = (LockItemInfo) other;
        return getPartitionKey().equals(o.getPartitionKey()) && getOwnerName().equals(o.getOwnerName());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{getPartitionKey(), getOwnerName()});
    }

    /**
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * <p>
     * This is equivalent to calling lockClient.sendHeartbeat(lockItem)
     */
    public void sendHeartBeat() {
        this.client.sendHeartbeat(this);
    }

    /**
     * <p>
     * Ensures that this owner has the lock for a specified period of time. If the lock will expire in less than the amount of
     * time passed in, then this method will do nothing. Otherwise, it will set the {@code leaseDuration} to that value and send a
     * heartbeat, such that the lock will expire no sooner than after {@code leaseDuration} elapses.
     * </p>
     * <p>
     * This method is not required if using heartbeats, because the client could simply call {@code isExpired} before every
     * operation to ensure that it still has the lock. However, it is possible for the client to instead call this method before
     * executing every operation if they all require different lengths of time, and the client wants to ensure it always has
     * enough time.
     * </p>
     * <p>
     * This method will throw a {@code LockNotGrantedException} if it does not currently hold the lock.
     * </p>
     *
     * @param leaseDurationToEnsure The amount of time to ensure that the lease is granted for
     * @param timeUnit              The time unit for the leaseDuration
     */
    public void ensure(final long leaseDurationToEnsure, final TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit, "TimeUnit cannot be null");
        if (isReleased()) {
            throw new LockNotGrantedException("Lock is released");
        }
        final long leaseDurationToEnsureInMilliseconds = timeUnit.toMillis(leaseDurationToEnsure);
        if (getLeaseDuration() - (com.amazonaws.services.dynamodbv2.util.LockClientUtils.INSTANCE.millisecondTime()
                - getLookupTime()) <= leaseDurationToEnsureInMilliseconds) {
            this.client.sendHeartbeat(SendHeartbeatOptions.builder(this)
                    .withLeaseDurationToEnsure(leaseDurationToEnsure).withTimeUnit(timeUnit).build());
        }
    }
}
