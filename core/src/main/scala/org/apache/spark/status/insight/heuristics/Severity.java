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
package org.apache.spark.status.insight.heuristics;

public enum Severity {
    Critical(3, "Must fix. Job would fail or has severe performance issue"),

    Normal(2, "Better fix. Job might fail or has severe resource inefficiency"),

    LOW(1, "Minor resource inefficiency"),

    NONE(0, "");

    private final int value;
    private final String tooltip;

    Severity(int value, String tooltip) {
        this.value = value;
        this.tooltip = tooltip;
    }

    public int getValue() {
        return value;
    }

    public String getTooltip() {
        return tooltip;
    }

    /**
     * Returns the maximum of the severities
     *
     * @param a One severity
     * @param b The other severity
     * @return Max(a,b)
     */
    public static Severity max(Severity a, Severity b) {
        if (a. value > b. value) {
            return a;
        }
        return b;
    }

    /**
     * Returns the maximum of the severities in the array
     *
     * @param severities Arbitrary number of severities
     * @return Max(severities)
     */
    public static Severity max(Severity... severities) {
        Severity currentSeverity = NONE;
        for (Severity severity : severities) {
            currentSeverity = max(currentSeverity, severity);
        }
        return currentSeverity;
    }

    /**
     * Returns the minimum of the severities
     *
     * @param a One severity
     * @param b The other severity
     * @return Min(a,b)
     */
    public static Severity min(Severity a, Severity b) {
        if (a. value < b. value) {
            return a;
        }
        return b;
    }

    /**
     * Returns the severity level of the value in the given thresholds
     * low < moderate < severe < critical
     *
     * Critical when value is greater than the critical threshold
     * None when the value is less than the low threshold.
     *
     * @param value The value being tested
     * @return One of the 5 severity levels
     */
    public static Severity getSeverityAscending(Number value, Number low, Number moderate, Number severe,
                                                Number critical) {
        if (value.doubleValue() >= critical.doubleValue()) {
            return Critical;
        }
        if (value.doubleValue() >= severe.doubleValue()) {
            return Normal;
        }
        if (value.doubleValue() >= moderate.doubleValue()) {
            return LOW;
        }
        if (value.doubleValue() >= low.doubleValue()) {
            return LOW;
        }
        return NONE;
    }

    /**
     * Returns the severity level of the value in the given thresholds
     * low > moderate > severe > critical
     *
     * Critical when value is less than the critical threshold
     * None when the value is greater than the low threshold.
     *
     * @param value The value being tested
     * @return One of the 5 severity levels
     */
    public static Severity getSeverityDescending(Number value, Number low, Number moderate, Number severe,
                                                 Number critical) {
        if (value.doubleValue() <= critical.doubleValue()) {
            return Critical;
        }
        if (value.doubleValue() <= severe.doubleValue()) {
            return Normal;
        }
        if (value.doubleValue() <= moderate.doubleValue()) {
            return LOW;
        }
        if (value.doubleValue() <= low.doubleValue()) {
            return LOW;
        }
        return NONE;
    }
}
