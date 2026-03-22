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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AmazonDynamoDBLockClientAsync}.
 * All DynamoDB calls are mocked via {@link DynamoDbAsyncClient}.
 */
public class AmazonDynamoDBLockClientAsyncTest {

    private static final String OWNER = "test-owner";
    private static final String TABLE = "locks";
    private static final String PK_NAME = "customer";
    private static final long LEASE_MS = 10_000L;
    private static final long HEARTBEAT_MS = 3_000L;

    private DynamoDbAsyncClient dynamoDB;
    private AmazonDynamoDBLockClientAsync client;
    private AmazonDynamoDBLockClientAsync clientWithHoldLock;

    @Before
    public void setUp() {
        dynamoDB = Mockito.mock(DynamoDbAsyncClient.class);
        // Default stub so tearDown's close() can always release any held locks.
        when(dynamoDB.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));
        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));
        client = buildClient(false);
        clientWithHoldLock = buildClient(true);
    }

    @After
    public void tearDown() {
        client.close();
        clientWithHoldLock.close();
    }

    // -------------------------------------------------------------------------
    // Helper: build client
    // -------------------------------------------------------------------------

    private AmazonDynamoDBLockClientAsync buildClient(boolean holdLockOnServiceUnavailable) {
        return new AmazonDynamoDBLockClientAsync(
            AmazonDynamoDBLockClientAsyncOptions.builder(dynamoDB, TABLE)
                .withOwnerName(OWNER)
                .withPartitionKeyName(PK_NAME)
                .withLeaseDuration(LEASE_MS)
                .withHeartbeatPeriod(HEARTBEAT_MS)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withCreateHeartbeatBackgroundThread(false)
                .withHoldLockOnServiceUnavailable(holdLockOnServiceUnavailable)
                .build());
    }

    /** Returns a map representing a live (un-released) lock owned by a third party. */
    private Map<String, AttributeValue> thirdPartyLockItem(String rvn, long leaseDurationMs) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK_NAME, AttributeValue.builder().s("customer1").build());
        item.put("ownerName", AttributeValue.builder().s("other-owner").build());
        item.put("leaseDuration", AttributeValue.builder().s(String.valueOf(leaseDurationMs)).build());
        item.put("recordVersionNumber", AttributeValue.builder().s(rvn).build());
        return item;
    }

    /** Returns a map representing a released lock. */
    private Map<String, AttributeValue> releasedLockItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK_NAME, AttributeValue.builder().s("customer1").build());
        item.put("ownerName", AttributeValue.builder().s("other-owner").build());
        item.put("leaseDuration", AttributeValue.builder().s("1").build());
        item.put("recordVersionNumber", AttributeValue.builder().s("some-rvn").build());
        item.put("isReleased", AttributeValue.builder().s("1").build());
        return item;
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    /** Unwraps ExecutionException and any RuntimeException wrappers, asserts expectedType found. */
    private static <T extends Throwable> T assertFutureThrows(
            Class<T> expectedType, CompletableFuture<?> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("Expected " + expectedType.getSimpleName() + " but future completed normally");
            return null;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            // Unwrap RuntimeException wrappers the async client may add
            while (cause != null && !expectedType.isInstance(cause)
                    && cause.getCause() != null && cause.getCause() != cause) {
                cause = cause.getCause();
            }
            assertTrue("Expected " + expectedType.getSimpleName() + " but got " + cause.getClass().getSimpleName(),
                    expectedType.isInstance(cause));
            return expectedType.cast(cause);
        }
    }

    // =========================================================================
    // lockTableExistsAsync
    // =========================================================================

    @Test
    public void lockTableExistsAsync_activeTable_returnsTrue() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                        .build()));

        assertTrue(client.lockTableExistsAsync().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void lockTableExistsAsync_updatingTable_returnsTrue() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.UPDATING).build())
                        .build()));

        assertTrue(client.lockTableExistsAsync().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void lockTableExistsAsync_creatingTable_returnsFalse() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.CREATING).build())
                        .build()));

        assertFalse(client.lockTableExistsAsync().get(5, TimeUnit.SECONDS));
    }

    @Test
    public void lockTableExistsAsync_resourceNotFound_returnsFalse() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(failedFuture(ResourceNotFoundException.builder().message("no table").build()));

        assertFalse(client.lockTableExistsAsync().get(5, TimeUnit.SECONDS));
    }

    // =========================================================================
    // assertLockTableExistsAsync
    // =========================================================================

    @Test
    public void assertLockTableExistsAsync_tableExists_completesNormally() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.ACTIVE).build())
                        .build()));

        // Should complete without exception
        client.assertLockTableExistsAsync().get(5, TimeUnit.SECONDS);
    }

    @Test
    public void assertLockTableExistsAsync_tableMissing_throwsLockTableDoesNotExistException() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(failedFuture(ResourceNotFoundException.builder().message("no table").build()));

        assertFutureThrows(LockTableDoesNotExistException.class, client.assertLockTableExistsAsync());
    }

    @Test
    public void assertLockTableExistsAsync_tableCreating_throwsLockTableDoesNotExistException() throws Exception {
        when(dynamoDB.describeTable(any(DescribeTableRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DescribeTableResponse.builder()
                        .table(TableDescription.builder().tableStatus(TableStatus.CREATING).build())
                        .build()));

        assertFutureThrows(LockTableDoesNotExistException.class, client.assertLockTableExistsAsync());
    }

    // =========================================================================
    // acquireLockAsync
    // =========================================================================

    @Test
    public void acquireLockAsync_noExistingLock_acquiresImmediately() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

        LockItem lock = client.acquireLockAsync(AcquireLockOptions.builder("customer1").build())
                .get(5, TimeUnit.SECONDS);

        assertNotNull(lock);
        assertEquals("customer1", lock.getPartitionKey());
        assertEquals(OWNER, lock.getOwnerName());
        verify(dynamoDB).putItem(any(PutItemRequest.class));
    }

    @Test
    public void acquireLockAsync_releasedLock_acquiresImmediately() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(releasedLockItem()).build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

        LockItem lock = client.acquireLockAsync(AcquireLockOptions.builder("customer1").build())
                .get(5, TimeUnit.SECONDS);

        assertNotNull(lock);
        assertEquals(OWNER, lock.getOwnerName());
    }

    @Test
    public void acquireLockAsync_conditionalCheckFailed_throwsLockNotGrantedException() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(failedFuture(ConditionalCheckFailedException.builder().message("conflict").build()));

        AcquireLockOptions opts = AcquireLockOptions.builder("customer1")
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).build();

        assertFutureThrows(LockNotGrantedException.class, client.acquireLockAsync(opts));
    }

    @Test
    public void acquireLockAsync_provisionedThroughputExceeded_throwsLockNotGrantedException() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(failedFuture(ProvisionedThroughputExceededException.builder().message("throttle").build()));

        AcquireLockOptions opts = AcquireLockOptions.builder("customer1")
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).build();

        assertFutureThrows(LockNotGrantedException.class, client.acquireLockAsync(opts));
    }

    @Test
    public void acquireLockAsync_shouldSkipBlockingWait_lockHeld_throwsLockCurrentlyUnavailableException() throws Exception {
        // Lock is actively held (not released, not expired) with a long lease
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(thirdPartyLockItem("rvn1", 600_000L)).build()));

        AcquireLockOptions opts = AcquireLockOptions.builder("customer1")
                .withShouldSkipBlockingWait(true)
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).build();

        assertFutureThrows(LockCurrentlyUnavailableException.class, client.acquireLockAsync(opts));
    }

    @Test
    public void acquireLockAsync_acquireOnlyIfExists_noLock_throwsLockNotGrantedException() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        AcquireLockOptions opts = AcquireLockOptions.builder("customer1")
                .withAcquireOnlyIfLockAlreadyExists(true)
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).build();

        assertFutureThrows(LockNotGrantedException.class, client.acquireLockAsync(opts));
    }

    @Test
    public void acquireLockAsync_heldLockRvnKeepsChanging_throwsLockNotGrantedException() throws Exception {
        // Simulate an actively heartbeated lock: RVN changes on every poll so we never
        // consider the lock expired and keep retrying until the budget runs out.
        // Lease = 150ms: mutableMsToWait becomes 0 + 150 = 150ms after the first
        // iteration. With refreshPeriod=10ms we exceed 150ms in ~15 retries (~150ms).
        final AtomicInteger counter = new AtomicInteger(0);
        when(dynamoDB.getItem(any(GetItemRequest.class))).thenAnswer(inv ->
                CompletableFuture.completedFuture(GetItemResponse.builder()
                        .item(thirdPartyLockItem("rvn-" + counter.incrementAndGet(), 150L))
                        .build()));

        AcquireLockOptions opts = AcquireLockOptions.builder("customer1")
                .withAdditionalTimeToWaitForLock(0L)
                .withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build();

        assertFutureThrows(LockNotGrantedException.class, client.acquireLockAsync(opts));
    }

    // =========================================================================
    // releaseLockAsync
    // =========================================================================

    /** Acquires a lock using the mock DDB client (putItem path). */
    private LockItem acquireLock(String partitionKey) throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        return client.acquireLockAsync(
                AcquireLockOptions.builder(partitionKey).withDeleteLockOnRelease(true).build())
                .get(5, TimeUnit.SECONDS);
    }

    @Test
    public void releaseLockAsync_deletePath_callsDeleteItemAndReturnsTrue() throws Exception {
        LockItem lock = acquireLock("customer1");
        when(dynamoDB.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        boolean released = client.releaseLockAsync(lock).get(5, TimeUnit.SECONDS);

        assertTrue(released);
        verify(dynamoDB).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    public void releaseLockAsync_updatePath_callsUpdateItemAndReturnsTrue() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        LockItem lock = client.acquireLockAsync(
                AcquireLockOptions.builder("customer1").withDeleteLockOnRelease(false).build())
                .get(5, TimeUnit.SECONDS);
        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        boolean released = client.releaseLockAsync(lock).get(5, TimeUnit.SECONDS);

        assertTrue(released);
        verify(dynamoDB).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void releaseLockAsync_conditionalCheckFailed_returnsFalse() throws Exception {
        LockItem lock = acquireLock("customer1");
        when(dynamoDB.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(failedFuture(ConditionalCheckFailedException.builder().message("conflict").build()));

        boolean released = client.releaseLockAsync(lock).get(5, TimeUnit.SECONDS);

        assertFalse(released);
    }

    @Test
    public void releaseLockAsync_bestEffortSdkClientException_returnsTrue() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        LockItem lock = client.acquireLockAsync(
                AcquireLockOptions.builder("customer1").withDeleteLockOnRelease(true).build())
                .get(5, TimeUnit.SECONDS);
        when(dynamoDB.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(failedFuture(SdkClientException.builder().message("network error").build()));

        boolean released = client.releaseLockAsync(
                ReleaseLockOptions.builder(lock).withDeleteLock(true).withBestEffort(true).build())
                .get(5, TimeUnit.SECONDS);

        assertTrue(released);
    }

    // =========================================================================
    // sendHeartbeatAsync
    // =========================================================================

    @Test
    public void sendHeartbeatAsync_success_updatesRecordVersionNumber() throws Exception {
        LockItem lock = acquireLock("customer1");
        String originalRvn = lock.getRecordVersionNumber();

        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        client.sendHeartbeatAsync(lock).get(5, TimeUnit.SECONDS);

        assertFalse("RVN should have changed after heartbeat",
                originalRvn.equals(lock.getRecordVersionNumber()));
        verify(dynamoDB).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    public void sendHeartbeatAsync_expiredLock_throwsLockNotGrantedException() throws Exception {
        // Use a client whose lease is 1 ms so the acquired lock expires immediately.
        AmazonDynamoDBLockClientAsync tinyLeaseClient = new AmazonDynamoDBLockClientAsync(
                AmazonDynamoDBLockClientAsyncOptions.builder(dynamoDB, TABLE)
                        .withOwnerName(OWNER).withPartitionKeyName(PK_NAME)
                        .withLeaseDuration(1L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.MILLISECONDS)
                        .withCreateHeartbeatBackgroundThread(false).build());
        try {
            when(dynamoDB.getItem(any(GetItemRequest.class)))
                    .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
            when(dynamoDB.putItem(any(PutItemRequest.class)))
                    .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));

            LockItem lock = tinyLeaseClient.acquireLockAsync(
                    AcquireLockOptions.builder("customer1").build()).get(5, TimeUnit.SECONDS);

            Thread.sleep(50); // wait past the 1 ms lease

            assertFutureThrows(LockNotGrantedException.class, tinyLeaseClient.sendHeartbeatAsync(lock));
        } finally {
            tinyLeaseClient.close();
        }
    }

    @Test
    public void sendHeartbeatAsync_conditionalCheckFailed_removesLockAndThrows() throws Exception {
        LockItem lock = acquireLock("customer1");
        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(failedFuture(ConditionalCheckFailedException.builder().message("conflict").build()));

        assertFutureThrows(LockNotGrantedException.class, client.sendHeartbeatAsync(lock));
    }

    @Test
    public void sendHeartbeatAsync_serviceUnavailableWithHoldLock_updatesLookupTimeAndReturns() throws Exception {
        // Build a client with holdLockOnServiceUnavailable = true
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        LockItem lock = clientWithHoldLock.acquireLockAsync(
                AcquireLockOptions.builder("customer1").build()).get(5, TimeUnit.SECONDS);

        AwsServiceException serviceUnavailable = (AwsServiceException) AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .statusCode(HttpStatusCode.SERVICE_UNAVAILABLE).build())
                        .build())
                .build();
        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(failedFuture(serviceUnavailable));

        // Should complete without exception when holdLockOnServiceUnavailable=true
        clientWithHoldLock.sendHeartbeatAsync(lock).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void sendHeartbeatAsync_serviceUnavailableWithoutHoldLock_propagatesException() throws Exception {
        LockItem lock = acquireLock("customer1");

        AwsServiceException serviceUnavailable = (AwsServiceException) AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .sdkHttpResponse(SdkHttpResponse.builder()
                                .statusCode(HttpStatusCode.SERVICE_UNAVAILABLE).build())
                        .build())
                .build();
        when(dynamoDB.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(failedFuture(serviceUnavailable));

        // Should propagate when holdLockOnServiceUnavailable=false
        assertFutureThrows(AwsServiceException.class, client.sendHeartbeatAsync(lock));
    }

    // =========================================================================
    // getLockAsync / getLockFromDynamoDBAsync
    // =========================================================================

    @Test
    public void getLockAsync_lockHeldLocally_returnsFromCache() throws Exception {
        LockItem lock = acquireLock("customer1");

        Optional<LockItem> result = client.getLockAsync("customer1", Optional.empty()).get(5, TimeUnit.SECONDS);

        assertTrue(result.isPresent());
        // Should return the same object from local cache (no extra getItem call)
        verify(dynamoDB, Mockito.times(1)).getItem(any(GetItemRequest.class));
    }

    @Test
    public void getLockAsync_notInCache_fetchesFromDynamoDB() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(thirdPartyLockItem("rvn1", LEASE_MS)).build()));

        Optional<LockItem> result = client.getLockAsync("customer1", Optional.empty()).get(5, TimeUnit.SECONDS);

        assertTrue(result.isPresent());
        verify(dynamoDB).getItem(any(GetItemRequest.class));
    }

    @Test
    public void getLockAsync_releasedLockInDynamoDB_returnsEmpty() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(releasedLockItem()).build()));

        Optional<LockItem> result = client.getLockAsync("customer1", Optional.empty()).get(5, TimeUnit.SECONDS);

        assertFalse(result.isPresent());
    }

    @Test
    public void getLockAsync_notFoundInDynamoDB_returnsEmpty() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));

        Optional<LockItem> result = client.getLockAsync("customer1", Optional.empty()).get(5, TimeUnit.SECONDS);

        assertFalse(result.isPresent());
    }

    @Test
    public void getLockAsync_fetchedLock_rvnClearedToPreventHeartbeat() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(thirdPartyLockItem("rvn1", LEASE_MS)).build()));

        Optional<LockItem> result = client.getLockAsync("customer1", Optional.empty()).get(5, TimeUnit.SECONDS);

        assertTrue(result.isPresent());
        assertEquals("", result.get().getRecordVersionNumber());
    }

    @Test
    public void getLockFromDynamoDBAsync_itemExists_returnsLockItem() throws Exception {
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        GetItemResponse.builder().item(thirdPartyLockItem("rvn1", LEASE_MS)).build()));

        GetLockOptions opts = new GetLockOptions.GetLockOptionsBuilder("customer1")
                .withDeleteLockOnRelease(false).build();
        Optional<LockItem> result = client.getLockFromDynamoDBAsync(opts).get(5, TimeUnit.SECONDS);

        assertTrue(result.isPresent());
        assertEquals("other-owner", result.get().getOwnerName());
    }

    // =========================================================================
    // closeAsync
    // =========================================================================

    @Test
    public void closeAsync_releasesAllHeldLocks() throws Exception {
        // Acquire two locks
        when(dynamoDB.getItem(any(GetItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(GetItemResponse.builder().build()));
        when(dynamoDB.putItem(any(PutItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(PutItemResponse.builder().build()));
        when(dynamoDB.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(DeleteItemResponse.builder().build()));

        client.acquireLockAsync(AcquireLockOptions.builder("key1").withDeleteLockOnRelease(true).build())
                .get(5, TimeUnit.SECONDS);
        client.acquireLockAsync(AcquireLockOptions.builder("key2").withDeleteLockOnRelease(true).build())
                .get(5, TimeUnit.SECONDS);

        client.closeAsync().get(5, TimeUnit.SECONDS);

        // Two deleteItem calls — one per lock
        verify(dynamoDB, Mockito.times(2)).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    public void closeAsync_noLocks_completesNormally() throws Exception {
        // No locks acquired — closeAsync should complete without error
        client.closeAsync().get(5, TimeUnit.SECONDS);
    }
}
