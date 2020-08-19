package org.apache.spark.status.insight.heuristics;

public enum Severity {
    Critical(3, "Must fix. Job would fail or has severe performance issue"),

    Normal(2, "Better fix. Job might fail or has severe resource inefficiency"),

    Low(1, "Minor resource inefficiency");

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
}
