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

import com.amazonaws.services.dynamodbv2.model.SessionMonitorNotSetException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A lock acquired via {@link AmazonDynamoDBLockClientAsync}. This is a pure
 * data object — it carries no reference to a client and exposes no blocking
 * operations. Use {@link AmazonDynamoDBLockClientAsync#releaseLockAsync} or
 * {@link AmazonDynamoDBLockClientAsync#runWithLockAsync} to release the lock.
 */
class LockItemInfo {

    private final String partitionKey;
    private final Optional<String> sortKey;

    private Optional<ByteBuffer> data;
    private final String ownerName;
    private final boolean deleteLockItemOnClose;
    private final boolean isReleased;
    private final AtomicLong lookupTime;
    private final StringBuffer recordVersionNumber;
    private final AtomicLong leaseDuration;
    private final Map<String, AttributeValue> additionalAttributes;

    private final Optional<SessionMonitor> sessionMonitor;

    /**
     * Package-private — constructed only by {@link AmazonDynamoDBLockClientAsync}.
     */
    LockItemInfo(final String partitionKey, final Optional<String> sortKey,
            final Optional<ByteBuffer> data, final boolean deleteLockItemOnClose,
            final String ownerName, final long leaseDuration,
            final long lastUpdatedTimeInMilliseconds, final String recordVersionNumber,
            final boolean isReleased, final Optional<SessionMonitor> sessionMonitor,
            final Map<String, AttributeValue> additionalAttributes) {
        Objects.requireNonNull(partitionKey, "Cannot create a lock with a null key");
        Objects.requireNonNull(ownerName, "Cannot create a lock with a null owner");
        Objects.requireNonNull(sortKey, "Cannot create a lock with a null sortKey (use Optional.empty())");
        Objects.requireNonNull(data, "Cannot create a lock with null data (use Optional.empty())");
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.data = data;
        this.ownerName = ownerName;
        this.deleteLockItemOnClose = deleteLockItemOnClose;
        this.leaseDuration = new AtomicLong(leaseDuration);
        this.lookupTime = new AtomicLong(lastUpdatedTimeInMilliseconds);
        this.recordVersionNumber = new StringBuffer(recordVersionNumber);
        this.isReleased = isReleased;
        this.sessionMonitor = sessionMonitor;
        this.additionalAttributes = additionalAttributes;
    }

    public String getPartitionKey() {
        return this.partitionKey;
    }

    public Optional<String> getSortKey() {
        return this.sortKey;
    }

    public Optional<ByteBuffer> getData() {
        return this.data;
    }

    public Map<String, AttributeValue> getAdditionalAttributes() {
        return this.additionalAttributes;
    }

    public String getOwnerName() {
        return this.ownerName;
    }

    public long getLookupTime() {
        return this.lookupTime.get();
    }

    public String getRecordVersionNumber() {
        return this.recordVersionNumber.toString();
    }

    public long getLeaseDuration() {
        return this.leaseDuration.get();
    }

    public boolean getDeleteLockItemOnClose() {
        return this.deleteLockItemOnClose;
    }

    /**
     * Returns whether the lock has expired based on the lease duration and last heartbeat time.
     */
    public boolean isExpired() {
        if (this.isReleased) {
            return true;
        }
        return LockClientUtils.INSTANCE.millisecondTime() - this.lookupTime.get() > this.leaseDuration.get();
    }

    /**
     * Returns whether the lock was marked as released when loaded from DynamoDB.
     */
    boolean isReleased() {
        return this.isReleased;
    }

    /**
     * Returns whether the lock is entering the "danger zone" period.
     *
     * @throws SessionMonitorNotSetException when the SessionMonitor is not set
     * @throws IllegalStateException         when the lock is already released
     */
    public boolean amIAboutToExpire() {
        return this.millisecondsUntilDangerZoneEntered() <= 0;
    }

    long millisecondsUntilDangerZoneEntered() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("SessionMonitor is not set");
        }
        if (this.isReleased) {
            throw new IllegalStateException("Lock is already released");
        }
        return this.sessionMonitor.get().millisecondsUntilLeaseEntersDangerZone(this.getLookupTime());
    }

    boolean hasSessionMonitor() {
        return this.sessionMonitor.isPresent();
    }

    boolean hasCallback() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("SessionMonitor is not set");
        }
        return this.sessionMonitor.get().hasCallback();
    }

    void runSessionMonitor() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("Can't run callback without first setting SessionMonitor");
        }
        this.sessionMonitor.get().runCallback();
    }

    void updateRecordVersionNumber(final String recordVersionNumber,
            final long lastUpdateOfLock, final long leaseDurationToEnsureInMilliseconds) {
        this.recordVersionNumber.replace(0, this.recordVersionNumber.length(), recordVersionNumber);
        this.lookupTime.set(lastUpdateOfLock);
        this.leaseDuration.set(leaseDurationToEnsureInMilliseconds);
    }

    void updateData(final ByteBuffer byteBuffer) {
        this.data = Optional.ofNullable(byteBuffer);
    }

    public void updateLookUpTime(final long lastUpdateOfLock) {
        this.lookupTime.set(lastUpdateOfLock);
    }

    String getUniqueIdentifier() {
        return this.partitionKey + this.sortKey.orElse("");
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(Arrays.asList(this.partitionKey, this.ownerName).toArray());
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || !(other instanceof LockItemInfo)) {
            return false;
        }
        final LockItemInfo o = (LockItemInfo) other;
        return this.partitionKey.equals(o.getPartitionKey()) && this.ownerName.equals(o.getOwnerName());
    }

    @Override
    public String toString() {
        String dataString = this.data
                .map(bb -> new String(bb.array(), StandardCharsets.UTF_8))
                .orElse("");
        return String.format(
                "LockItemInfo{Partition Key=%s, Sort Key=%s, Owner Name=%s, Lookup Time=%d, "
                        + "Lease Duration=%d, Record Version Number=%s, Delete On Close=%s, "
                        + "Data=%s, Is Released=%s}",
                this.partitionKey, this.sortKey, this.ownerName, this.lookupTime.get(),
                this.leaseDuration.get(), this.recordVersionNumber, this.deleteLockItemOnClose,
                dataString, this.isReleased);
    }
}
