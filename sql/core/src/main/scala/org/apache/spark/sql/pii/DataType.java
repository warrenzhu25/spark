/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.pii;

public enum DataType {

    /**
     * Data will be deleted within 30 days, and 24 hours invisible requirement doesn't apply.
     * MT platform will delete the data after 30 days expiration.
     * All the downstream copies should inherit the original long tail file creation time.
     */
    LongTail,

    /**
     * Data will retain more than 30 days, while 24 hours invisible doesn't apply.
     * MT platform will apply 30 days hard delete requirement for data subject's request.
     */
    IntermediateEngineering,

    /**
     * Data will be used for personalization, recommendation, ads, etc.
     * It will apply 24 hours invisible requirement.
     * For this MT choose to hard delete within 24 hours.
     */
    Personalization,

    /**
     * Non personal data will written in output
     */
    NonPersonal
}
