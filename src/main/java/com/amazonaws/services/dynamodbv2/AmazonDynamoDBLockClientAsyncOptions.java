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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Options for constructing an {@link AmazonDynamoDBLockClientAsync}. Mirrors
 * {@link AmazonDynamoDBLockClientOptions} but accepts a {@link DynamoDbAsyncClient}
 * and an optional {@link ScheduledExecutorService} for heartbeating and session
 * monitoring instead of a {@code ThreadFactory}.
 */
public class AmazonDynamoDBLockClientAsyncOptions {
    static final String DEFAULT_PARTITION_KEY_NAME = "key";
    static final Long DEFAULT_LEASE_DURATION = 20L;
    static final Long DEFAULT_HEARTBEAT_PERIOD = 5L;
    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    static final Boolean DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD = true;
    static final Boolean DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE = false;

    private final DynamoDbAsyncClient dynamoDBAsyncClient;
    private final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;
    private final String ownerName;
    private final Long leaseDuration;
    private final Long heartbeatPeriod;
    private final TimeUnit timeUnit;
    private final Boolean createHeartbeatBackgroundThread;
    private final Boolean holdLockOnServiceUnavailable;
    private final Optional<ScheduledExecutorService> heartbeatExecutor;

    /**
     * Builder for {@link AmazonDynamoDBLockClientAsyncOptions}.
     */
    public static class AmazonDynamoDBLockClientAsyncOptionsBuilder {
        private DynamoDbAsyncClient dynamoDBAsyncClient;
        private String tableName;
        private String partitionKeyName;
        private Optional<String> sortKeyName;
        private String ownerName;
        private Long leaseDuration;
        private Long heartbeatPeriod;
        private TimeUnit timeUnit;
        private Boolean createHeartbeatBackgroundThread;
        private Boolean holdLockOnServiceUnavailable;
        private Optional<ScheduledExecutorService> heartbeatExecutor;

        AmazonDynamoDBLockClientAsyncOptionsBuilder(final DynamoDbAsyncClient dynamoDBAsyncClient,
                final String tableName) {
            this.dynamoDBAsyncClient = dynamoDBAsyncClient;
            this.tableName = tableName;
            this.partitionKeyName = DEFAULT_PARTITION_KEY_NAME;
            this.leaseDuration = DEFAULT_LEASE_DURATION;
            this.heartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
            this.timeUnit = DEFAULT_TIME_UNIT;
            this.createHeartbeatBackgroundThread = DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD;
            this.sortKeyName = Optional.empty();
            this.ownerName = generateOwnerNameFromLocalhost();
            this.holdLockOnServiceUnavailable = DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE;
            this.heartbeatExecutor = Optional.empty();
        }

        private static String generateOwnerNameFromLocalhost() {
            try {
                return Inet4Address.getLocalHost().getHostName() + UUID.randomUUID().toString();
            } catch (final UnknownHostException e) {
                return UUID.randomUUID().toString();
            }
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withPartitionKeyName(final String partitionKeyName) {
            this.partitionKeyName = partitionKeyName;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withSortKeyName(final String sortKeyName) {
            this.sortKeyName = Optional.of(sortKeyName);
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withOwnerName(final String ownerName) {
            this.ownerName = ownerName;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withLeaseDuration(final Long leaseDuration) {
            this.leaseDuration = leaseDuration;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withHeartbeatPeriod(final Long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withTimeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withCreateHeartbeatBackgroundThread(
                final Boolean createHeartbeatBackgroundThread) {
            this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptionsBuilder withHoldLockOnServiceUnavailable(
                final Boolean holdLockOnServiceUnavailable) {
            this.holdLockOnServiceUnavailable = holdLockOnServiceUnavailable;
            return this;
        }

        /**
         * Injects a custom {@link ScheduledExecutorService} used for heartbeating,
         * session monitors, and acquire-lock retry delays. If not provided, a
         * single-thread executor is created automatically and shut down on
         * {@link AmazonDynamoDBLockClientAsync#close()}.
         */
        public AmazonDynamoDBLockClientAsyncOptionsBuilder withHeartbeatExecutor(
                final ScheduledExecutorService heartbeatExecutor) {
            this.heartbeatExecutor = Optional.ofNullable(heartbeatExecutor);
            return this;
        }

        public AmazonDynamoDBLockClientAsyncOptions build() {
            Objects.requireNonNull(this.tableName, "Table Name must not be null");
            Objects.requireNonNull(this.ownerName, "Owner Name must not be null");
            return new AmazonDynamoDBLockClientAsyncOptions(this.dynamoDBAsyncClient, this.tableName,
                    this.partitionKeyName, this.sortKeyName, this.ownerName, this.leaseDuration,
                    this.heartbeatPeriod, this.timeUnit, this.createHeartbeatBackgroundThread,
                    this.holdLockOnServiceUnavailable, this.heartbeatExecutor);
        }
    }

    public static AmazonDynamoDBLockClientAsyncOptionsBuilder builder(
            final DynamoDbAsyncClient dynamoDBAsyncClient, final String tableName) {
        return new AmazonDynamoDBLockClientAsyncOptionsBuilder(dynamoDBAsyncClient, tableName);
    }

    private AmazonDynamoDBLockClientAsyncOptions(final DynamoDbAsyncClient dynamoDBAsyncClient,
            final String tableName, final String partitionKeyName, final Optional<String> sortKeyName,
            final String ownerName, final Long leaseDuration, final Long heartbeatPeriod,
            final TimeUnit timeUnit, final Boolean createHeartbeatBackgroundThread,
            final Boolean holdLockOnServiceUnavailable,
            final Optional<ScheduledExecutorService> heartbeatExecutor) {
        this.dynamoDBAsyncClient = dynamoDBAsyncClient;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.sortKeyName = sortKeyName;
        this.ownerName = ownerName;
        this.leaseDuration = leaseDuration;
        this.heartbeatPeriod = heartbeatPeriod;
        this.timeUnit = timeUnit;
        this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
        this.holdLockOnServiceUnavailable = holdLockOnServiceUnavailable;
        this.heartbeatExecutor = heartbeatExecutor;
    }

    DynamoDbAsyncClient getDynamoDBAsyncClient() {
        return this.dynamoDBAsyncClient;
    }

    String getTableName() {
        return this.tableName;
    }

    String getPartitionKeyName() {
        return this.partitionKeyName;
    }

    Optional<String> getSortKeyName() {
        return this.sortKeyName;
    }

    String getOwnerName() {
        return this.ownerName;
    }

    Long getLeaseDuration() {
        return this.leaseDuration;
    }

    Long getHeartbeatPeriod() {
        return this.heartbeatPeriod;
    }

    TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    Boolean getCreateHeartbeatBackgroundThread() {
        return this.createHeartbeatBackgroundThread;
    }

    Boolean getHoldLockOnServiceUnavailable() {
        return this.holdLockOnServiceUnavailable;
    }

    Optional<ScheduledExecutorService> getHeartbeatExecutor() {
        return this.heartbeatExecutor;
    }
}
