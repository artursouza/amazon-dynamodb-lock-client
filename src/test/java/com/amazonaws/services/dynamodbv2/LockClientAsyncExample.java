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

import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Usage example for {@link AmazonDynamoDBLockClientAsync}.
 * Start DynamoDB Local on port 4567 before running this example (happens
 * automatically with {@code mvn clean install verify -Pintegration-tests}).
 */
public class LockClientAsyncExample {

    @Test
    public void usageExample() throws Exception {
        // 1. Build the async DynamoDB client pointing at DynamoDB Local.
        final DynamoDbAsyncClient dynamoDB = DynamoDbAsyncClient.builder()
                .region(Region.US_WEST_2)
                .endpointOverride(URI.create("http://localhost:4567"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("local", "local")))
                .build();

        // 2. Create the lock table if it does not already exist.
        try {
            AmazonDynamoDBLockClientAsync.createLockTableInDynamoDBAsync(
                    dynamoDB,
                    ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build(),
                    "lockTable",
                    "key",
                    Optional.empty()).join();
        } catch (Exception e) {
            if (!(e.getCause() instanceof ResourceInUseException)) {
                throw e;
            }
        }

        // 3. Build the async lock client. Using try-with-resources ensures that all
        //    held locks are released and the scheduler is shut down on exit.
        try (AmazonDynamoDBLockClientAsync client = new AmazonDynamoDBLockClientAsync(
                AmazonDynamoDBLockClientAsyncOptions.builder(dynamoDB, "lockTable")
                        .withTimeUnit(TimeUnit.SECONDS)
                        .withLeaseDuration(10L)
                        .withHeartbeatPeriod(3L)
                        .withCreateHeartbeatBackgroundThread(true)
                        .build())) {

            // 4a. runWithLockAsync — acquires the lock, runs the work once, releases on completion.
            //     The lock is always released, even if the work throws.
            client.runWithLockAsync("Moe", () -> {
                System.out.println("Acquired lock! If I die, my lock will expire in 10 seconds.");
                System.out.println("Otherwise, I will hold it until I stop heartbeating.");
                return CompletableFuture.completedFuture(null);
            }).join();

            // 4b. runWithLockAsync with a return value.
            final String result = client.<String>runWithLockAsync("Larry", () -> {
                System.out.println("Lock for Larry acquired — doing protected work.");
                // ... perform protected work here ...
                return CompletableFuture.completedFuture("done");
            }).join();
            System.out.println("Lock for Larry work result: " + result);
        }
    }

    /**
     * Demonstrates acquiring a lock with a session-monitor callback that fires
     * when the lock is approaching expiry and has not been heartbeated recently.
     */
    @Test
    public void sessionMonitorExample() throws Exception {
        final DynamoDbAsyncClient dynamoDB = DynamoDbAsyncClient.builder()
                .region(Region.US_WEST_2)
                .endpointOverride(URI.create("http://localhost:4567"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("local", "local")))
                .build();

        try {
            AmazonDynamoDBLockClientAsync.createLockTableInDynamoDBAsync(
                    dynamoDB,
                    ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build(),
                    "lockTable",
                    "key",
                    Optional.empty()).join();
        } catch (Exception e) {
            if (!(e.getCause() instanceof ResourceInUseException)) {
                throw e;
            }
        }

        try (AmazonDynamoDBLockClientAsync client = new AmazonDynamoDBLockClientAsync(
                AmazonDynamoDBLockClientAsyncOptions.builder(dynamoDB, "lockTable")
                        .withTimeUnit(TimeUnit.SECONDS)
                        .withLeaseDuration(10L)
                        .withHeartbeatPeriod(3L)
                        .withCreateHeartbeatBackgroundThread(true)
                        .build())) {

            // The session monitor fires when the lock has not been heartbeated for
            // (leaseDuration - safeTimeWithoutHeartbeat) = 10s - 4s = 6s.
            client.runWithLockAsync(
                    AcquireLockOptions.builder("Curly")
                            .withSessionMonitor(4L, Optional.of(() ->
                                    System.out.println("WARNING: lock for Curly is close to expiring!")))
                            .withTimeUnit(TimeUnit.SECONDS)
                            .build(),
                    () -> {
                        System.out.println("Acquired lock for Curly with session monitor.");
                        return CompletableFuture.completedFuture(null);
                    }).get();
        }
    }
}
