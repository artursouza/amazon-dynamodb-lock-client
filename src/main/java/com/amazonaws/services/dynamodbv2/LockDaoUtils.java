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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Stateless utility methods shared by the sync and async lock clients.
 * Package-private — not part of the public API.
 */
final class LockDaoUtils {

    private LockDaoUtils() {}

    /**
     * Builds the DynamoDB item key map for the given partition key (and optional sort key).
     */
    static Map<String, AttributeValue> buildItemKey(
            final String partitionKeyName, final String partitionKey,
            final Optional<String> sortKeyName, final Optional<String> sortKey) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(partitionKeyName, AttributeValue.builder().s(partitionKey).build());
        if (sortKey.isPresent()) {
            key.put(sortKeyName.get(), AttributeValue.builder().s(sortKey.get()).build());
        }
        return key;
    }

    /**
     * Builds a {@code SET #k0=:v0,#k1=:v1,...} update expression from the given attribute map,
     * populating {@code exprNames} and {@code exprValues} as a side-effect.
     */
    static String buildUpdateExpression(
            final Map<String, AttributeValue> item,
            final Map<String, String> exprNames,
            final Map<String, AttributeValue> exprValues) {
        final StringBuilder sb = new StringBuilder("SET ");
        final Iterator<Map.Entry<String, AttributeValue>> it = item.entrySet().iterator();
        int i = 0;
        while (it.hasNext()) {
            final Map.Entry<String, AttributeValue> entry = it.next();
            exprNames.put("#k" + i, entry.getKey());
            exprValues.put(":v" + i, entry.getValue());
            sb.append("#k").append(i).append("=").append(":v").append(i);
            if (it.hasNext()) {
                sb.append(",");
            }
            i++;
        }
        return sb.toString();
    }

    /**
     * Resolves the data payload to write into a new lock item.
     * If {@code replaceData} is true, always use {@code optionsData}.
     * Otherwise prefer {@code existingData} (carry it forward), falling back to {@code optionsData}.
     */
    static Optional<ByteBuffer> resolveNewLockData(
            final boolean replaceData,
            final Optional<ByteBuffer> existingData,
            final Optional<ByteBuffer> optionsData) {
        Optional<ByteBuffer> result = Optional.empty();
        if (replaceData) {
            result = optionsData;
        } else if (existingData.isPresent()) {
            result = existingData;
        }
        if (!result.isPresent()) {
            result = optionsData;
        }
        return result;
    }

    /** Generates a UUID string for use as a record version number. */
    static String generateRecordVersionNumber() {
        return UUID.randomUUID().toString();
    }

    /**
     * Holds the result of {@link #buildNewLockItem}: the populated attribute map and the
     * freshly generated record version number (needed separately by the caller to construct
     * the in-memory {@code LockItem}).
     */
    static final class NewLockItem {
        final Map<String, AttributeValue> item;
        final String recordVersionNumber;

        NewLockItem(final Map<String, AttributeValue> item, final String recordVersionNumber) {
            this.item = item;
            this.recordVersionNumber = recordVersionNumber;
        }
    }

    /**
     * Builds the DynamoDB attribute map for a brand-new lock item and generates its
     * record version number. Used identically by both the sync and async acquire paths.
     */
    static NewLockItem buildNewLockItem(
            final String partitionKeyName, final String key,
            final String ownerName, final long leaseDurationInMilliseconds,
            final Optional<String> sortKeyName, final Optional<String> sortKey,
            final Optional<ByteBuffer> data,
            final Map<String, AttributeValue> additionalAttributes) {
        final Map<String, AttributeValue> item = new HashMap<>(additionalAttributes);
        item.put(partitionKeyName, AttributeValue.builder().s(key).build());
        item.put(LockDaoConstants.OWNER_NAME, AttributeValue.builder().s(ownerName).build());
        item.put(LockDaoConstants.LEASE_DURATION, AttributeValue.builder().s(String.valueOf(leaseDurationInMilliseconds)).build());
        final String recordVersionNumber = generateRecordVersionNumber();
        item.put(LockDaoConstants.RECORD_VERSION_NUMBER, AttributeValue.builder().s(recordVersionNumber).build());
        sortKeyName.ifPresent(sk -> item.put(sk, AttributeValue.builder().s(sortKey.get()).build()));
        data.ifPresent(bb -> item.put(LockDaoConstants.DATA, AttributeValue.builder().b(SdkBytes.fromByteBuffer(bb)).build()));
        return new NewLockItem(item, recordVersionNumber);
    }

    /**
     * Validates session-monitor arguments.
     *
     * @throws IllegalArgumentException if {@code safeTimeMs} is not strictly between
     *                                  {@code heartbeatPeriodMs} and {@code leaseDurationMs}
     */
    static void sessionMonitorArgsValidate(
            final long safeTimeMs, final long heartbeatPeriodMs, final long leaseDurationMs) {
        if (safeTimeMs <= heartbeatPeriodMs) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be greater than heartbeat frequency");
        }
        if (safeTimeMs >= leaseDurationMs) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be less than the lock's lease duration");
        }
    }

    /**
     * Builds a {@link CreateTableRequest} for a DynamoDB lock table. The returned request
     * can be passed directly to either a sync or async DynamoDB client.
     */
    static CreateTableRequest buildCreateTableRequest(
            final String tableName,
            final String partitionKeyName,
            final Optional<String> sortKeyName,
            final ProvisionedThroughput provisionedThroughput) {
        final List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(KeySchemaElement.builder()
                .attributeName(partitionKeyName).keyType(KeyType.HASH).build());
        final Collection<AttributeDefinition> attrDefs = new ArrayList<>();
        attrDefs.add(AttributeDefinition.builder()
                .attributeName(partitionKeyName).attributeType(ScalarAttributeType.S).build());
        if (sortKeyName.isPresent()) {
            keySchema.add(KeySchemaElement.builder()
                    .attributeName(sortKeyName.get()).keyType(KeyType.RANGE).build());
            attrDefs.add(AttributeDefinition.builder()
                    .attributeName(sortKeyName.get()).attributeType(ScalarAttributeType.S).build());
        }
        return CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(keySchema)
                .provisionedThroughput(provisionedThroughput)
                .attributeDefinitions(attrDefs)
                .build();
    }
}
