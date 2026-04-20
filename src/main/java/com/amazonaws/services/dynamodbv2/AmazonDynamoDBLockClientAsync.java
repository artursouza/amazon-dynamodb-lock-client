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

import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import static com.amazonaws.services.dynamodbv2.LockDaoConstants.*;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Async version of the DynamoDB lock client. All DynamoDB operations return
 * {@link CompletableFuture}; no thread is ever blocked waiting on network I/O.
 *
 * <p>Heartbeating and session monitors run on a shared {@link ScheduledExecutorService}.
 * The acquire-lock polling loop schedules retries on the same executor instead of
 * calling {@code Thread.sleep}.
 *
 * <p>This class is thread-safe.
 */
@ThreadSafe
public class AmazonDynamoDBLockClientAsync implements Closeable {

    private static final Log logger = LogFactory.getLog(AmazonDynamoDBLockClientAsync.class);

    private static final Set<TableStatus> AVAILABLE_STATUSES = new HashSet<>();
    static {
        AVAILABLE_STATUSES.add(TableStatus.ACTIVE);
        AVAILABLE_STATUSES.add(TableStatus.UPDATING);
    }

    /** Sentinel thrown (never escaping the class) to signal "retry the poll loop". */
    private static final class NeedToRetryException extends RuntimeException {
        NeedToRetryException() { super(null, null, true, false); }
    }
    private static final NeedToRetryException NEED_TO_RETRY = new NeedToRetryException();

    // -------------------------------------------------------------------------
    // Instance fields
    // -------------------------------------------------------------------------

    private final DynamoDbAsyncClient dynamoDB;
    private final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;
    private final long leaseDurationInMilliseconds;
    private final long heartbeatPeriodInMilliseconds;
    private final boolean holdLockOnServiceUnavailable;
    private final String ownerName;

    private final ConcurrentHashMap<String, LockItemInfo> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LockItemInfo> notMyLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> sessionMonitors = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    /** True when the executor was created by this client and must be shut down on close. */
    private final boolean ownsScheduler;
    private volatile boolean shuttingDown = false;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public AmazonDynamoDBLockClientAsync(final AmazonDynamoDBLockClientAsyncOptions options) {
        Objects.requireNonNull(options.getDynamoDBAsyncClient(), "DynamoDB async client cannot be null");
        Objects.requireNonNull(options.getTableName(), "Table name cannot be null");
        Objects.requireNonNull(options.getOwnerName(), "Owner name cannot be null");
        Objects.requireNonNull(options.getTimeUnit(), "Time unit cannot be null");
        Objects.requireNonNull(options.getPartitionKeyName(), "Partition key name cannot be null");
        Objects.requireNonNull(options.getSortKeyName(), "Sort key name cannot be null (use Optional.empty())");

        this.dynamoDB = options.getDynamoDBAsyncClient();
        this.tableName = options.getTableName();
        this.ownerName = options.getOwnerName();
        this.leaseDurationInMilliseconds = options.getTimeUnit().toMillis(options.getLeaseDuration());
        this.heartbeatPeriodInMilliseconds = options.getTimeUnit().toMillis(options.getHeartbeatPeriod());
        this.partitionKeyName = options.getPartitionKeyName();
        this.sortKeyName = options.getSortKeyName();
        this.holdLockOnServiceUnavailable = options.getHoldLockOnServiceUnavailable();

        if (options.getHeartbeatExecutor().isPresent()) {
            this.scheduler = options.getHeartbeatExecutor().get();
            this.ownsScheduler = false;
        } else {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dynamodb-lock-client-async-scheduler");
                t.setDaemon(true);
                return t;
            });
            this.ownsScheduler = true;
        }

        if (options.getCreateHeartbeatBackgroundThread()) {
            if (this.leaseDurationInMilliseconds < 2 * this.heartbeatPeriodInMilliseconds) {
                throw new IllegalArgumentException(
                        "Heartbeat period must be no more than half the length of the Lease Duration");
            }
            scheduleNextHeartbeatRound(0L);
        }
    }

    // -------------------------------------------------------------------------
    // Public async API
    // -------------------------------------------------------------------------

    /**
     * Asynchronously acquires a lock, polling DynamoDB until the lock is obtained
     * or {@code additionalTimeToWaitForLock} elapses. No thread is blocked during
     * the wait; retries are scheduled on the internal {@link ScheduledExecutorService}.
     */
    CompletableFuture<LockItemInfo> acquireLockAsync(final AcquireLockOptions options) {
        Objects.requireNonNull(options, "Cannot acquire lock when options is null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot acquire lock when key is null");

        final String key = options.getPartitionKey();
        final Optional<String> sortKey = options.getSortKey();

        if (options.getReentrant() && hasLock(key, sortKey)) {
            final LockItemInfo local = this.locks.get(key + sortKey.orElse(""));
            if (local != null && !local.isExpired()) {
                return CompletableFuture.completedFuture(local);
            }
        }

        if (options.getAdditionalAttributes().containsKey(this.partitionKeyName)
                || options.getAdditionalAttributes().containsKey(OWNER_NAME)
                || options.getAdditionalAttributes().containsKey(LEASE_DURATION)
                || options.getAdditionalAttributes().containsKey(RECORD_VERSION_NUMBER)
                || options.getAdditionalAttributes().containsKey(DATA)
                || this.sortKeyName.isPresent()
                        && options.getAdditionalAttributes().containsKey(this.sortKeyName.get())) {
            throw new IllegalArgumentException("Additional attribute cannot be one of the reserved lock attributes");
        }

        long millisecondsToWait = DEFAULT_BUFFER_MS;
        if (options.getAdditionalTimeToWaitForLock() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if additionalTimeToWaitForLock is non-null");
            millisecondsToWait = options.getTimeUnit().toMillis(options.getAdditionalTimeToWaitForLock());
        }
        long refreshPeriodMs = DEFAULT_BUFFER_MS;
        if (options.getRefreshPeriod() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if refreshPeriod is non-null");
            refreshPeriodMs = options.getTimeUnit().toMillis(options.getRefreshPeriod());
        }

        final Optional<SessionMonitor> sessionMonitor = options.getSessionMonitor();
        if (sessionMonitor.isPresent()) {
            LockDaoUtils.sessionMonitorArgsValidate(sessionMonitor.get().getSafeTimeMillis(),
                    this.heartbeatPeriodInMilliseconds, this.leaseDurationInMilliseconds);
        }

        final boolean deleteLockOnRelease = options.getDeleteLockOnRelease();
        final long startTimeMs = LockClientUtils.INSTANCE.millisecondTime();
        final AtomicLong mutableMsToWait = new AtomicLong(millisecondsToWait);
        final AtomicReference<LockItemInfo> lockTryingToBeAcquired = new AtomicReference<>(null);
        final AtomicBoolean alreadySleptOnce = new AtomicBoolean(false);
        final GetLockOptions getLockOptions = new GetLockOptions.GetLockOptionsBuilder(key)
                .withSortKey(sortKey.orElse(null))
                .withDeleteLockOnRelease(deleteLockOnRelease)
                .build();
        final long finalRefreshPeriodMs = refreshPeriodMs;

        final CompletableFuture<LockItemInfo> promise = new CompletableFuture<>();
        runAcquireIteration(promise, options, key, sortKey, startTimeMs, mutableMsToWait,
                finalRefreshPeriodMs, lockTryingToBeAcquired, alreadySleptOnce,
                getLockOptions, deleteLockOnRelease, sessionMonitor);
        return promise;
    }

    private void runAcquireIteration(
            CompletableFuture<LockItemInfo> promise, AcquireLockOptions options,
            String key, Optional<String> sortKey,
            long startTimeMs, AtomicLong mutableMsToWait, long refreshPeriodMs,
            AtomicReference<LockItemInfo> lockTryingToBeAcquired, AtomicBoolean alreadySleptOnce,
            GetLockOptions getLockOptions, boolean deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor) {

        if (promise.isDone()) {
            return;
        }

        getLockFromDynamoDBAsync(getLockOptions)
                .thenCompose(existingLock -> handleAcquireWithExistingLock(
                        existingLock, options, key, sortKey,
                        mutableMsToWait, lockTryingToBeAcquired, alreadySleptOnce,
                        deleteLockOnRelease, sessionMonitor))
                .whenComplete((acquired, ex) -> {
                    if (promise.isDone()) {
                        return;
                    }
                    if (ex == null) {
                        promise.complete(acquired);
                        return;
                    }

                    Throwable cause = unwrap(ex);
                    // Convert DDB-level failures to LockNotGrantedException where appropriate
                    if (cause instanceof ConditionalCheckFailedException) {
                        logger.debug("Someone else acquired the lock", cause);
                        cause = new LockNotGrantedException("Could not acquire lock because someone else acquired it: ", cause);
                    } else if (cause instanceof ProvisionedThroughputExceededException) {
                        logger.debug("Provisioned throughput exceeded", cause);
                        cause = new LockNotGrantedException("Could not acquire lock because provisioned throughput exceeded", cause);
                    }

                    final long elapsed = LockClientUtils.INSTANCE.millisecondTime() - startTimeMs;
                    final boolean timedOut = elapsed > mutableMsToWait.get();

                    if (cause instanceof SdkClientException) {
                        logger.warn("Could not acquire lock because of a client side failure in talking to DDB", cause);
                        // Retry unless timed out
                    } else if (cause instanceof LockNotGrantedException || cause instanceof NeedToRetryException) {
                        if (timedOut) {
                            promise.completeExceptionally(cause instanceof NeedToRetryException
                                    ? new LockNotGrantedException("Didn't acquire lock after sleeping for " + elapsed + " milliseconds")
                                    : cause);
                            return;
                        }
                        // else: retry
                    } else {
                        promise.completeExceptionally(cause);
                        return;
                    }

                    if (timedOut) {
                        promise.completeExceptionally(new LockNotGrantedException(
                                "Didn't acquire lock after sleeping for " + elapsed + " milliseconds"));
                        return;
                    }

                    logger.trace("Scheduling acquire retry after " + refreshPeriodMs + " ms");
                    scheduler.schedule(() -> runAcquireIteration(promise, options, key, sortKey,
                            startTimeMs, mutableMsToWait, refreshPeriodMs,
                            lockTryingToBeAcquired, alreadySleptOnce,
                            getLockOptions, deleteLockOnRelease, sessionMonitor),
                            refreshPeriodMs, TimeUnit.MILLISECONDS);
                });
    }

    private CompletableFuture<LockItemInfo> handleAcquireWithExistingLock(
            Optional<LockItemInfo> existingLock, AcquireLockOptions options,
            String key, Optional<String> sortKey,
            AtomicLong mutableMsToWait, AtomicReference<LockItemInfo> lockTryingToBeAcquired,
            AtomicBoolean alreadySleptOnce, boolean deleteLockOnRelease,
            Optional<SessionMonitor> sessionMonitor) {

        if (options.getAcquireOnlyIfLockAlreadyExists() && !existingLock.isPresent()) {
            return failedFuture(new LockNotGrantedException("Lock does not exist."));
        }

        if (options.shouldSkipBlockingWait() && existingLock.isPresent() && !existingLock.get().isExpired()) {
            final String id = existingLock.get().getUniqueIdentifier();
            boolean isReallyExpired = false;
            if (notMyLocks.containsKey(id)
                    && notMyLocks.get(id).getRecordVersionNumber().equals(existingLock.get().getRecordVersionNumber())) {
                isReallyExpired = notMyLocks.get(id).isExpired();
                if (isReallyExpired) {
                    lockTryingToBeAcquired.set(notMyLocks.get(id));
                }
            } else {
                notMyLocks.put(id, existingLock.get());
            }
            if (!isReallyExpired) {
                return failedFuture(new LockCurrentlyUnavailableException(
                        "The lock being requested is being held by another client."));
            }
        }

        final Optional<ByteBuffer> newLockData = LockDaoUtils.resolveNewLockData(
                options.getReplaceData(), existingLock.map(LockItemInfo::getData).flatMap(d -> d), options.getData());

        final LockDaoUtils.NewLockItem newLock = LockDaoUtils.buildNewLockItem(
                this.partitionKeyName, key, this.ownerName, this.leaseDurationInMilliseconds,
                this.sortKeyName, sortKey, newLockData, options.getAdditionalAttributes());
        final Map<String, AttributeValue> item = newLock.item;
        final String recordVersionNumber = newLock.recordVersionNumber;
        final Optional<ByteBuffer> finalNewLockData = newLockData;

        if (!existingLock.isPresent() && !options.getAcquireOnlyIfLockAlreadyExists()) {
            return upsertAndMonitorNewLockAsync(options, key, sortKey, deleteLockOnRelease,
                    sessionMonitor, finalNewLockData, item, recordVersionNumber);
        } else if (existingLock.isPresent() && existingLock.get().isReleased()) {
            return upsertAndMonitorReleasedLockAsync(options, key, sortKey, deleteLockOnRelease,
                    sessionMonitor, existingLock, finalNewLockData, item, recordVersionNumber);
        }

        final LockItemInfo ltba = lockTryingToBeAcquired.get();
        if (ltba == null) {
            lockTryingToBeAcquired.set(existingLock.get());
            if (!alreadySleptOnce.getAndSet(true)) {
                mutableMsToWait.addAndGet(existingLock.get().getLeaseDuration());
            }
            return failedFuture(NEED_TO_RETRY);
        } else {
            if (ltba.getRecordVersionNumber().equals(existingLock.get().getRecordVersionNumber())) {
                if (ltba.isExpired()) {
                    return upsertAndMonitorExpiredLockAsync(options, key, sortKey, deleteLockOnRelease,
                            sessionMonitor, existingLock, finalNewLockData, item, recordVersionNumber);
                }
            } else {
                lockTryingToBeAcquired.set(existingLock.get());
            }
            return failedFuture(NEED_TO_RETRY);
        }
    }

    /**
     * Attempts to acquire a lock, returning an empty {@link Optional} instead of
     * throwing if the lock cannot be granted.
     */
    CompletableFuture<Optional<LockItemInfo>> tryAcquireLockAsync(final AcquireLockOptions options) {
        return acquireLockAsync(options)
                .thenApply(Optional::of)
                .exceptionally(ex -> {
                    if (unwrap(ex) instanceof LockNotGrantedException) {
                        return Optional.empty();
                    }
                    if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                    throw new RuntimeException(ex);
                });
    }

    /**
     * Returns true if this client currently holds the lock for the given key.
     */
    boolean hasLock(final String key, final Optional<String> sortKey) {
        Objects.requireNonNull(sortKey, "Sort Key must not be null (can be Optional.empty())");
        final LockItemInfo local = this.locks.get(key + sortKey.orElse(""));
        return local != null && !local.isExpired();
    }

    /** Releases the lock, deleting or marking it as released per the lock's own setting. */
    CompletableFuture<Boolean> releaseLockAsync(final LockItemInfo lockItem) {
        Objects.requireNonNull(lockItem, "Cannot release null lockItem");

        if (!lockItem.getOwnerName().equals(this.ownerName)) {
            return CompletableFuture.completedFuture(false);
        }

        // Remove from heartbeat map immediately — mirrors sync client behaviour.
        this.locks.remove(lockItem.getUniqueIdentifier());

        final boolean deleteLock = lockItem.getDeleteLockItemOnClose();

        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        final Map<String, String> expressionAttributeNames = new HashMap<>();
        final String conditionalExpression;
        final Map<String, AttributeValue> itemKey;

        synchronized (lockItem) {
            expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE,
                    AttributeValue.builder().s(lockItem.getRecordVersionNumber()).build());
            expressionAttributeValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE,
                    AttributeValue.builder().s(lockItem.getOwnerName()).build());
            expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
            expressionAttributeNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
            expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            if (this.sortKeyName.isPresent()) {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
            } else {
                conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
            }
            itemKey = getItemKeys(lockItem);
        }

        final CompletableFuture<Void> ddbCall;
        if (deleteLock) {
            ddbCall = this.dynamoDB.deleteItem(DeleteItemRequest.builder()
                    .tableName(tableName).key(itemKey)
                    .conditionExpression(conditionalExpression)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).build())
                    .thenAccept(r -> { });
        } else {
            expressionAttributeNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
            expressionAttributeValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);
            ddbCall = this.dynamoDB.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName).key(itemKey)
                    .updateExpression(UPDATE_IS_RELEASED)
                    .conditionExpression(conditionalExpression)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues).build())
                    .thenAccept(r -> { });
        }

        return ddbCall
                .thenApply(v -> {
                    removeKillSessionMonitor(lockItem.getUniqueIdentifier());
                    return true;
                })
                .exceptionally(ex -> {
                    final Throwable cause = unwrap(ex);
                    if (cause instanceof ConditionalCheckFailedException) {
                        logger.debug("Someone else acquired the lock before you asked to release it", cause);
                        return false;
                    }
                    if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                    throw new RuntimeException(ex);
                });
    }

    /** Sends a heartbeat for the given lock, refreshing its lease duration. */
    CompletableFuture<Void> sendHeartbeatAsync(final LockItemInfo lockItem) {
        Objects.requireNonNull(lockItem, "Cannot send heartbeat for null lock");
        final long finalLeaseDurationMs = this.leaseDurationInMilliseconds;

        if (lockItem.isExpired() || !lockItem.getOwnerName().equals(this.ownerName) || lockItem.isReleased()) {
            this.locks.remove(lockItem.getUniqueIdentifier());
            return failedFuture(new LockNotGrantedException("Cannot send heartbeat because lock is not granted"));
        }

        final UpdateItemRequest updateItemRequest;
        final String newRvn;

        synchronized (lockItem) {
            final Map<String, AttributeValue> exprValues = new HashMap<>();
            exprValues.put(RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getRecordVersionNumber()).build());
            exprValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(lockItem.getOwnerName()).build());
            final Map<String, String> exprNames = new HashMap<>();
            exprNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
            exprNames.put(LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION);
            exprNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            exprNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
            final String condExpr;
            if (this.sortKeyName.isPresent()) {
                condExpr = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                exprNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
            } else {
                condExpr = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
            }
            newRvn = LockDaoUtils.generateRecordVersionNumber();
            exprValues.put(NEW_RVN_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(newRvn).build());
            exprValues.put(LEASE_DURATION_VALUE_EXPRESSION_VARIABLE,
                    AttributeValue.builder().s(String.valueOf(finalLeaseDurationMs)).build());
            final String updateExpr = UPDATE_LEASE_DURATION_AND_RVN;
            updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName).key(getItemKeys(lockItem))
                    .conditionExpression(condExpr).updateExpression(updateExpr)
                    .expressionAttributeNames(exprNames).expressionAttributeValues(exprValues).build();
        }

        final long lastUpdate = LockClientUtils.INSTANCE.millisecondTime();
        final String capturedRvn = newRvn;

        return this.dynamoDB.updateItem(updateItemRequest)
                .thenAccept(resp -> {
                    synchronized (lockItem) {
                        lockItem.updateRecordVersionNumber(capturedRvn, lastUpdate, finalLeaseDurationMs);
                    }
                })
                .exceptionally(ex -> {
                    final Throwable cause = unwrap(ex);
                    if (cause instanceof ConditionalCheckFailedException) {
                        logger.debug("Someone else acquired the lock, so we will stop heartbeating it", cause);
                        this.locks.remove(lockItem.getUniqueIdentifier());
                        throw new RuntimeException(
                                new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it", cause));
                    }
                    if (cause instanceof AwsServiceException) {
                        final AwsServiceException aws = (AwsServiceException) cause;
                        if (this.holdLockOnServiceUnavailable
                                && aws.awsErrorDetails().sdkHttpResponse().statusCode() == HttpStatusCode.SERVICE_UNAVAILABLE) {
                            logger.info("DynamoDB Service Unavailable. Holding the lock.");
                            lockItem.updateLookUpTime(LockClientUtils.INSTANCE.millisecondTime());
                            return null;
                        }
                    }
                    if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                    throw new RuntimeException(ex);
                });
    }

    /**
     * Returns the lock if currently held locally or retrieves it from DynamoDB.
     * Clears the RVN so callers cannot accidentally heartbeat a lock they don't own.
     */
    CompletableFuture<Optional<LockItemInfo>> getLockAsync(final String key, final Optional<String> sortKey) {
        Objects.requireNonNull(sortKey, "Sort Key must not be null (can be Optional.empty())");
        final LockItemInfo local = this.locks.get(key + sortKey.orElse(""));
        if (local != null) {
            return CompletableFuture.completedFuture(Optional.of(local));
        }
        return getLockFromDynamoDBAsync(new GetLockOptions.GetLockOptionsBuilder(key)
                .withSortKey(sortKey.orElse(null)).withDeleteLockOnRelease(false).build())
                .thenApply(lockItem -> {
                    if (!lockItem.isPresent()) {
                        return Optional.<LockItemInfo>empty();
                    }
                    if (lockItem.get().isReleased()) {
                        return Optional.<LockItemInfo>empty();
                    }
                    lockItem.get().updateRecordVersionNumber("", 0, lockItem.get().getLeaseDuration());
                    return lockItem;
                });
    }

    /** Reads a lock item directly from DynamoDB without acquiring it. */
    CompletableFuture<Optional<LockItemInfo>> getLockFromDynamoDBAsync(final GetLockOptions options) {
        Objects.requireNonNull(options, "GetLockOptions cannot be null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot lookup null key");

        final Map<String, AttributeValue> ddbKey = new HashMap<>();
        ddbKey.put(this.partitionKeyName, AttributeValue.builder().s(options.getPartitionKey()).build());
        if (this.sortKeyName.isPresent()) {
            ddbKey.put(this.sortKeyName.get(),
                    AttributeValue.builder().s(options.getSortKey().get()).build());
        }
        final GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName).key(ddbKey).consistentRead(true).build();

        return this.dynamoDB.getItem(request).thenApply(response -> {
            final Map<String, AttributeValue> item = response.item();
            if (item == null || item.isEmpty()) {
                return Optional.<LockItemInfo>empty();
            }
            return Optional.of(createLockItem(options, item));
        });
    }

    /** Returns all locks in the table as a list (fetches all pages). */
    CompletableFuture<List<LockItemInfo>> getAllLocksFromDynamoDBAsync(final boolean deleteOnRelease) {
        final ScanRequest request = ScanRequest.builder().tableName(this.tableName).build();
        return scanAllPagesAsync(request, new ArrayList<>(), deleteOnRelease);
    }

    private CompletableFuture<List<LockItemInfo>> scanAllPagesAsync(
            final ScanRequest request, final List<LockItemInfo> accumulated, final boolean deleteOnRelease) {
        return this.dynamoDB.scan(request).thenCompose(response -> {
            response.items().forEach(item -> {
                final String key = item.get(this.partitionKeyName).s();
                accumulated.add(buildLockItemFromScanResult(key, deleteOnRelease, item));
            });
            if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
                return scanAllPagesAsync(request.toBuilder()
                        .exclusiveStartKey(response.lastEvaluatedKey()).build(), accumulated, deleteOnRelease);
            }
            return CompletableFuture.completedFuture(accumulated);
        });
    }

    /** Returns all locks for a given partition key as a list (fetches all pages). */
    CompletableFuture<List<LockItemInfo>> getLocksByPartitionKeyAsync(
            final String key, final boolean deleteOnRelease) {
        final Map<String, String> exprNames = new HashMap<>();
        exprNames.put(PK_PATH_EXPRESSION_VARIABLE, this.partitionKeyName);
        final Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(PK_VALUE_EXPRESSION_VARIABLE, AttributeValue.builder().s(key).build());
        final QueryRequest request = QueryRequest.builder()
                .tableName(this.tableName)
                .keyConditionExpression(QUERY_PK_EXPRESSION)
                .expressionAttributeNames(exprNames)
                .expressionAttributeValues(exprValues).build();
        return queryAllPagesAsync(request, new ArrayList<>(), key, deleteOnRelease);
    }

    private CompletableFuture<List<LockItemInfo>> queryAllPagesAsync(
            final QueryRequest request, final List<LockItemInfo> accumulated,
            final String partitionKey, final boolean deleteOnRelease) {
        return this.dynamoDB.query(request).thenCompose(response -> {
            response.items().forEach(item ->
                    accumulated.add(buildLockItemFromScanResult(partitionKey, deleteOnRelease, item)));
            if (response.lastEvaluatedKey() != null && !response.lastEvaluatedKey().isEmpty()) {
                return queryAllPagesAsync(request.toBuilder()
                        .exclusiveStartKey(response.lastEvaluatedKey()).build(),
                        accumulated, partitionKey, deleteOnRelease);
            }
            return CompletableFuture.completedFuture(accumulated);
        });
    }

    CompletableFuture<Boolean> lockTableExistsAsync() {
        return this.dynamoDB.describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                .thenApply(r -> AVAILABLE_STATUSES.contains(r.table().tableStatus()))
                .exceptionally(ex -> {
                    if (unwrap(ex) instanceof ResourceNotFoundException) {
                        return false;
                    }
                    if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                    throw new RuntimeException(ex);
                });
    }

    CompletableFuture<Void> assertLockTableExistsAsync() {
        return lockTableExistsAsync()
                .thenAccept(exists -> {
                    if (!exists) {
                        throw new RuntimeException(
                                new LockTableDoesNotExistException("Lock table " + this.tableName + " does not exist"));
                    }
                })
                .exceptionally(ex -> {
                    final Throwable cause = unwrap(ex);
                    if (cause instanceof LockTableDoesNotExistException) {
                        if (ex instanceof RuntimeException) throw (RuntimeException) ex;
                        throw new RuntimeException(ex);
                    }
                    throw new RuntimeException(
                            new LockTableDoesNotExistException("Lock table " + this.tableName + " does not exist", cause));
                });
    }

    /**
     * Creates a DynamoDB table suitable for use with this lock client.
     */
    public static CompletableFuture<Void> createLockTableInDynamoDBAsync(
            final DynamoDbAsyncClient dynamoDB,
            final ProvisionedThroughput provisionedThroughput,
            final String tableName,
            final String partitionKeyName,
            final Optional<String> sortKeyName) {
        Objects.requireNonNull(dynamoDB, "DynamoDB client cannot be null");
        Objects.requireNonNull(tableName, "Table name cannot be null");
        Objects.requireNonNull(provisionedThroughput, "Provisioned throughput cannot be null");
        Objects.requireNonNull(partitionKeyName, "Partition key name cannot be null");
        return dynamoDB.createTable(
                LockDaoUtils.buildCreateTableRequest(tableName, partitionKeyName, sortKeyName, provisionedThroughput))
                .thenAccept(r -> { });
    }

    /**
     * Releases all held locks and shuts down the internal scheduler.
     * Returns a {@link CompletableFuture} that completes when all releases finish.
     */
    public CompletableFuture<Void> closeAsync() {
        this.shuttingDown = true;
        final List<CompletableFuture<?>> releases = new ArrayList<>(this.locks.values()).stream()
                .map(this::releaseLockAsync)
                .collect(Collectors.toList());
        return CompletableFuture.allOf(releases.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ownsScheduler) {
                        scheduler.shutdown();
                    }
                    if (ex != null) {
                        logger.warn("Exceptions occurred while releasing locks during close", ex);
                    }
                });
    }

    /** Blocking close for try-with-resources. Calls {@link #closeAsync()} and waits. */
    @Override
    public void close() {
        closeAsync().join();
    }

    /**
     * Acquires the lock for {@code key}, executes {@code work} once, then releases
     * the lock — the async equivalent of try-with-resources. The lock is always
     * released regardless of whether {@code work} succeeds or fails.
     *
     * <pre>{@code
     * client.runWithLockAsync("myKey", () -> doProtectedWorkAsync());
     * }</pre>
     */
    public <T> CompletableFuture<T> runWithLockAsync(
            final String key,
            final java.util.function.Supplier<CompletableFuture<T>> work) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(work, "work must not be null");
        return runWithLockAsync(AcquireLockOptions.builder(key).build(), work);
    }

    /**
     * Acquires the lock using {@code options} (for advanced configuration such as session
     * monitors), executes {@code work} once, then releases the lock. The lock is always
     * released regardless of whether {@code work} succeeds or fails.
     *
     * <pre>{@code
     * client.runWithLockAsync(
     *     AcquireLockOptions.builder("myKey")
     *         .withSessionMonitor(4L, Optional.of(callback))
     *         .withTimeUnit(TimeUnit.SECONDS)
     *         .build(),
     *     () -> doProtectedWorkAsync());
     * }</pre>
     */
    public <T> CompletableFuture<T> runWithLockAsync(
            final AcquireLockOptions options,
            final java.util.function.Supplier<CompletableFuture<T>> work) {
        Objects.requireNonNull(options, "options must not be null");
        Objects.requireNonNull(work, "work must not be null");
        return acquireLockAsync(options)
                .thenCompose(lock ->
                        work.get()
                                .whenComplete((result, ex) -> releaseLockAsync(lock)));
    }

    /**
     * Acquires the lock for {@code key}, then runs {@code work} in a continuous
     * loop until the returned future is completed (cancelled or failed), or until
     * the lock expires. The lock is always released when the loop terminates.
     *
     * <p>To stop the loop, complete the returned future:
     * <pre>{@code
     * CompletableFuture<Void> loop = client.loopWithLockAsync("myKey", () -> doWorkAsync());
     * // ... later:
     * loop.complete(null);  // stop gracefully
     * }</pre>
     */
    public CompletableFuture<Void> loopWithLockAsync(
            final String key,
            final java.util.function.Supplier<CompletableFuture<Void>> work) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(work, "work must not be null");
        final CompletableFuture<Void> loopFuture = new CompletableFuture<>();
        acquireLockAsync(AcquireLockOptions.builder(key).build())
                .whenComplete((lock, acquireEx) -> {
                    if (acquireEx != null) {
                        loopFuture.completeExceptionally(unwrap(acquireEx));
                        return;
                    }
                    loopFuture.whenComplete((v, ex) -> releaseLockAsync(lock));
                    runLoopWithLockIteration(lock, work, loopFuture);
                });
        return loopFuture;
    }

    private void runLoopWithLockIteration(
            final LockItemInfo lock,
            final java.util.function.Supplier<CompletableFuture<Void>> work,
            final CompletableFuture<Void> loopFuture) {
        if (loopFuture.isDone()) {
            return;
        }
        if (lock.isExpired()) {
            loopFuture.completeExceptionally(
                    new LockNotGrantedException("Lock expired during loop execution"));
            return;
        }
        try {
            work.get().whenComplete((v, ex) -> {
                if (loopFuture.isDone()) {
                    return;
                }
                if (ex != null) {
                    loopFuture.completeExceptionally(unwrap(ex));
                    return;
                }
                runLoopWithLockIteration(lock, work, loopFuture);
            });
        } catch (final Exception ex) {
            loopFuture.completeExceptionally(ex);
        }
    }

    // -------------------------------------------------------------------------
    // Private — heartbeat scheduler
    // -------------------------------------------------------------------------

    private void scheduleNextHeartbeatRound(final long delayMs) {
        if (shuttingDown) {
            return;
        }
        scheduler.schedule(this::executeHeartbeatRound, delayMs, TimeUnit.MILLISECONDS);
    }

    private void executeHeartbeatRound() {
        if (shuttingDown) {
            return;
        }
        final long start = LockClientUtils.INSTANCE.millisecondTime();
        final List<CompletableFuture<Void>> heartbeats = new ArrayList<>(this.locks.values()).stream()
                .map(lock -> sendHeartbeatAsync(lock)
                        .exceptionally(ex -> {
                            final Throwable cause = unwrap(ex);
                            if (cause instanceof LockNotGrantedException) {
                                logger.debug("Heartbeat failed for lock " + lock.getUniqueIdentifier(), cause);
                            } else {
                                logger.warn("Exception sending heartbeat for lock " + lock.getUniqueIdentifier(), cause);
                            }
                            return null;
                        }))
                .collect(Collectors.toList());

        CompletableFuture.allOf(heartbeats.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (!shuttingDown) {
                        final long elapsed = LockClientUtils.INSTANCE.millisecondTime() - start;
                        scheduleNextHeartbeatRound(Math.max(heartbeatPeriodInMilliseconds - elapsed, 0L));
                    }
                });
    }

    // -------------------------------------------------------------------------
    // Private — session monitor management
    // -------------------------------------------------------------------------

    private void tryAddSessionMonitor(final String lockName, final LockItemInfo lock) {
        if (lock.hasSessionMonitor() && lock.hasCallback()) {
            scheduleSessionMonitor(lockName, lock);
        }
    }

    private void scheduleSessionMonitor(final String lockName, final LockItemInfo lock) {
        final long delayMs = Math.max(lock.millisecondsUntilDangerZoneEntered(), 0L);
        final ScheduledFuture<?> future = scheduler.schedule(() -> {
            if (lock.millisecondsUntilDangerZoneEntered() <= 0) {
                lock.runSessionMonitor();
                sessionMonitors.remove(lockName);
            } else {
                // Heartbeat pushed back the expiry — reschedule
                scheduleSessionMonitor(lockName, lock);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
        sessionMonitors.put(lockName, future);
    }

    private void removeKillSessionMonitor(final String monitorName) {
        final ScheduledFuture<?> future = sessionMonitors.remove(monitorName);
        if (future != null) {
            future.cancel(true);
        }
    }

    // -------------------------------------------------------------------------
    // Private — acquire-lock upsert helpers
    // -------------------------------------------------------------------------

    private CompletableFuture<LockItemInfo> upsertAndMonitorNewLockAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<ByteBuffer> newLockData, Map<String, AttributeValue> item,
            String recordVersionNumber) {

        final Map<String, String> exprNames = new HashMap<>();
        exprNames.put(PK_PATH_EXPRESSION_VARIABLE, this.partitionKeyName);
        final String condExpr;
        if (this.sortKeyName.isPresent()) {
            condExpr = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION;
            exprNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            condExpr = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION;
        }

        if (options.getUpdateExistingLockRecord()) {
            item.remove(partitionKeyName);
            sortKeyName.ifPresent(item::remove);
            final Map<String, AttributeValue> exprValues = new HashMap<>();
            final String updateExpr = LockDaoUtils.buildUpdateExpression(item, exprNames, exprValues);
            return updateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease,
                    sessionMonitor, newLockData, recordVersionNumber,
                    UpdateItemRequest.builder().tableName(tableName).key(LockDaoUtils.buildItemKey(this.partitionKeyName, key, this.sortKeyName, sortKey))
                            .updateExpression(updateExpr).expressionAttributeNames(exprNames)
                            .expressionAttributeValues(exprValues).conditionExpression(condExpr).build());
        } else {
            return putLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease,
                    sessionMonitor, newLockData, recordVersionNumber,
                    PutItemRequest.builder().item(item).tableName(tableName)
                            .conditionExpression(condExpr).expressionAttributeNames(exprNames).build());
        }
    }

    private CompletableFuture<LockItemInfo> upsertAndMonitorReleasedLockAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<LockItemInfo> existingLock, Optional<ByteBuffer> newLockData,
            Map<String, AttributeValue> item, String recordVersionNumber) {

        final boolean consistentLockData = options.getAcquireReleasedLocksConsistently();
        final Map<String, String> exprNames = new HashMap<>();
        final Map<String, AttributeValue> exprValues = new HashMap<>();
        if (consistentLockData) {
            exprValues.put(RVN_VALUE_EXPRESSION_VARIABLE,
                    AttributeValue.builder().s(existingLock.get().getRecordVersionNumber()).build());
            exprNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        }
        exprNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        exprNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
        exprValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

        final String condExpr;
        if (this.sortKeyName.isPresent()) {
            exprNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
            condExpr = consistentLockData
                    ? PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION
                    : PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION;
        } else {
            condExpr = consistentLockData
                    ? PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION
                    : PK_EXISTS_AND_IS_RELEASED_CONDITION;
        }

        return upsertOrPutExistingLockAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor,
                existingLock, newLockData, item, recordVersionNumber,
                REMOVE_IS_RELEASED_UPDATE_EXPRESSION, exprNames, exprValues, condExpr);
    }

    private CompletableFuture<LockItemInfo> upsertAndMonitorExpiredLockAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<LockItemInfo> existingLock, Optional<ByteBuffer> newLockData,
            Map<String, AttributeValue> item, String recordVersionNumber) {

        final Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(RVN_VALUE_EXPRESSION_VARIABLE,
                AttributeValue.builder().s(existingLock.get().getRecordVersionNumber()).build());
        final Map<String, String> exprNames = new HashMap<>();
        exprNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        exprNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        final String condExpr;
        if (this.sortKeyName.isPresent()) {
            condExpr = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
            exprNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            condExpr = PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
        }

        return upsertOrPutExistingLockAsync(options, key, sortKey, deleteLockOnRelease, sessionMonitor,
                existingLock, newLockData, item, recordVersionNumber,
                "", exprNames, exprValues, condExpr);
    }

    /**
     * Shared terminal step for released-lock and expired-lock acquisition paths.
     * When {@code updateExistingLockRecord} is true, issues an UpdateItem with
     * {@code baseUpdateExpr + extraUpdateExpr}; otherwise issues a PutItem.
     */
    private CompletableFuture<LockItemInfo> upsertOrPutExistingLockAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<LockItemInfo> existingLock, Optional<ByteBuffer> newLockData,
            Map<String, AttributeValue> item, String recordVersionNumber,
            String extraUpdateExpr,
            Map<String, String> exprNames, Map<String, AttributeValue> exprValues,
            String condExpr) {
        if (options.getUpdateExistingLockRecord()) {
            item.remove(partitionKeyName);
            sortKeyName.ifPresent(item::remove);
            final String updateExpr = LockDaoUtils.buildUpdateExpression(item, exprNames, exprValues) + extraUpdateExpr;
            return updateItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease,
                    sessionMonitor, newLockData, recordVersionNumber,
                    UpdateItemRequest.builder().tableName(tableName).key(getItemKeys(existingLock.get()))
                            .updateExpression(updateExpr).expressionAttributeNames(exprNames)
                            .expressionAttributeValues(exprValues).conditionExpression(condExpr).build());
        }
        return putLockItemAndStartSessionMonitorAsync(options, key, sortKey, deleteLockOnRelease,
                sessionMonitor, newLockData, recordVersionNumber,
                PutItemRequest.builder().item(item).tableName(tableName)
                        .conditionExpression(condExpr).expressionAttributeNames(exprNames)
                        .expressionAttributeValues(exprValues).build());
    }

    private CompletableFuture<LockItemInfo> putLockItemAndStartSessionMonitorAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<ByteBuffer> newLockData, String recordVersionNumber,
            PutItemRequest request) {
        // Capture time BEFORE the DDB call — errs on the side of expiring sooner.
        final long lastUpdated = LockClientUtils.INSTANCE.millisecondTime();
        return this.dynamoDB.putItem(request).thenApply(resp -> {
            final LockItemInfo lockItem = new LockItemInfo(key, sortKey, newLockData,
                    deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds,
                    lastUpdated, recordVersionNumber, false, sessionMonitor,
                    options.getAdditionalAttributes());
            this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
            tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
            return lockItem;
        });
    }

    private CompletableFuture<LockItemInfo> updateItemAndStartSessionMonitorAsync(
            AcquireLockOptions options, String key, Optional<String> sortKey,
            boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
            Optional<ByteBuffer> newLockData, String recordVersionNumber,
            UpdateItemRequest request) {
        final long lastUpdated = LockClientUtils.INSTANCE.millisecondTime();
        return this.dynamoDB.updateItem(request).thenApply(resp -> {
            final LockItemInfo lockItem = new LockItemInfo(key, sortKey, newLockData,
                    deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds,
                    lastUpdated, recordVersionNumber, false, sessionMonitor,
                    options.getAdditionalAttributes());
            this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
            tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
            return lockItem;
        });
    }

    // -------------------------------------------------------------------------
    // Private — misc helpers
    // -------------------------------------------------------------------------

    private LockItemInfo createLockItem(final GetLockOptions options, final Map<String, AttributeValue> immutableItem) {
        final Map<String, AttributeValue> item = new HashMap<>(immutableItem);
        final Optional<ByteBuffer> data = Optional.ofNullable(item.remove(DATA))
                .map(av -> av.b().asByteBuffer());
        final AttributeValue ownerNameAv = item.remove(OWNER_NAME);
        final AttributeValue leaseDurationAv = item.remove(LEASE_DURATION);
        final AttributeValue rvnAv = item.remove(RECORD_VERSION_NUMBER);
        final boolean isReleased = item.containsKey(IS_RELEASED);
        item.remove(IS_RELEASED);
        item.remove(this.partitionKeyName);
        final long lookupTime = LockClientUtils.INSTANCE.millisecondTime();
        return new LockItemInfo(options.getPartitionKey(), options.getSortKey(), data,
                options.isDeleteLockOnRelease(), ownerNameAv.s(),
                Long.parseLong(leaseDurationAv.s()), lookupTime, rvnAv.s(),
                isReleased, Optional.empty(), item);
    }

    private LockItemInfo buildLockItemFromScanResult(
            final String key, final boolean deleteOnRelease, final Map<String, AttributeValue> item) {
        GetLockOptions.GetLockOptionsBuilder builder =
                GetLockOptions.builder(key).withDeleteLockOnRelease(deleteOnRelease);
        builder = this.sortKeyName.map(item::get).map(AttributeValue::s)
                .map(builder::withSortKey).orElse(builder);
        return createLockItem(builder.build(), item);
    }

    private Map<String, AttributeValue> getItemKeys(final LockItemInfo lockItem) {
        return LockDaoUtils.buildItemKey(this.partitionKeyName, lockItem.getPartitionKey(),
                this.sortKeyName, lockItem.getSortKey());
    }

    private static Throwable unwrap(final Throwable t) {
        if (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    private static <T> CompletableFuture<T> failedFuture(final Throwable t) {
        final CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }
}
