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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * DynamoDB expression-variable names, condition expressions, attribute-name
 * constants, and other string literals shared by both the sync and async lock
 * clients. Package-private — not part of the public API.
 */
final class LockDaoConstants {

    private LockDaoConstants() {}

    // -------------------------------------------------------------------------
    // Expression path / value variable names
    // -------------------------------------------------------------------------

    static final String SK_PATH_EXPRESSION_VARIABLE = "#sk";
    static final String PK_PATH_EXPRESSION_VARIABLE = "#pk";
    static final String PK_VALUE_EXPRESSION_VARIABLE = ":pk";
    static final String NEW_RVN_VALUE_EXPRESSION_VARIABLE = ":newRvn";
    static final String LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE = "#ld";
    static final String LEASE_DURATION_VALUE_EXPRESSION_VARIABLE = ":ld";
    static final String RVN_PATH_EXPRESSION_VARIABLE = "#rvn";
    static final String RVN_VALUE_EXPRESSION_VARIABLE = ":rvn";
    static final String OWNER_NAME_PATH_EXPRESSION_VARIABLE = "#on";
    static final String OWNER_NAME_VALUE_EXPRESSION_VARIABLE = ":on";
    static final String DATA_PATH_EXPRESSION_VARIABLE = "#d";
    static final String DATA_VALUE_EXPRESSION_VARIABLE = ":d";
    static final String IS_RELEASED_PATH_EXPRESSION_VARIABLE = "#ir";
    static final String IS_RELEASED_VALUE_EXPRESSION_VARIABLE = ":ir";

    // -------------------------------------------------------------------------
    // Condition expressions
    // -------------------------------------------------------------------------

    static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION =
            String.format("attribute_not_exists(%s)", PK_PATH_EXPRESSION_VARIABLE);

    static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION =
            String.format("attribute_not_exists(%s) AND attribute_not_exists(%s)",
                    PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_IS_RELEASED_CONDITION =
            String.format("attribute_exists(%s) AND %s = %s",
                    PK_PATH_EXPRESSION_VARIABLE,
                    IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION =
            String.format("attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
                    PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE,
                    IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION =
            String.format("attribute_exists(%s) AND attribute_exists(%s) AND %s = %s AND %s = %s",
                    PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE,
                    RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
                    IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
            String.format("attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
                    PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE,
                    RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
            String.format("%s AND %s = %s ",
                    PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION,
                    OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION =
            String.format("(attribute_exists(%s) AND %s = %s AND %s = %s)",
                    PK_PATH_EXPRESSION_VARIABLE,
                    RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
                    IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
            String.format("attribute_exists(%s) AND %s = %s",
                    PK_PATH_EXPRESSION_VARIABLE,
                    RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    static final String PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
            String.format("%s AND %s = %s",
                    PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION,
                    OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    // -------------------------------------------------------------------------
    // Update expressions
    // -------------------------------------------------------------------------

    static final String UPDATE_IS_RELEASED =
            String.format("SET %s = %s",
                    IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    static final String UPDATE_LEASE_DURATION_AND_RVN =
            String.format("SET %s = %s, %s = %s",
                    LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION_VALUE_EXPRESSION_VARIABLE,
                    RVN_PATH_EXPRESSION_VARIABLE, NEW_RVN_VALUE_EXPRESSION_VARIABLE);

    static final String REMOVE_IS_RELEASED_UPDATE_EXPRESSION =
            String.format(" REMOVE %s ", IS_RELEASED_PATH_EXPRESSION_VARIABLE);

    static final String QUERY_PK_EXPRESSION =
            String.format("%s = %s", PK_PATH_EXPRESSION_VARIABLE, PK_VALUE_EXPRESSION_VARIABLE);

    // -------------------------------------------------------------------------
    // DynamoDB attribute names
    // -------------------------------------------------------------------------

    static final String DATA = "data";
    static final String OWNER_NAME = "ownerName";
    static final String LEASE_DURATION = "leaseDuration";
    static final String RECORD_VERSION_NUMBER = "recordVersionNumber";
    static final String IS_RELEASED = "isReleased";
    static final String IS_RELEASED_VALUE = "1";
    static final AttributeValue IS_RELEASED_ATTRIBUTE_VALUE =
            AttributeValue.builder().s(IS_RELEASED_VALUE).build();

    static final long DEFAULT_BUFFER_MS = 1000;
}
