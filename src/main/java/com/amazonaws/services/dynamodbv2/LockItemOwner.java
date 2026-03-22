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

/**
 * Interface implemented by lock clients that own {@link LockItem} instances.
 * Allows {@link LockItem} to be used with both the synchronous
 * {@link AmazonDynamoDBLockClient} and the asynchronous
 * {@link AmazonDynamoDBLockClientAsync} without a hard dependency on either.
 */
interface LockItemOwner {
    /**
     * Releases the given lock. Returns true if successfully released, false if
     * the lock was already stolen by another owner.
     */
    boolean releaseLock(LockItem lockItem);

    /**
     * Sends a heartbeat for the given lock using default options.
     */
    void sendHeartbeat(LockItem lockItem);

    /**
     * Sends a heartbeat for the given lock using the provided options.
     */
    void sendHeartbeat(SendHeartbeatOptions options);
}
