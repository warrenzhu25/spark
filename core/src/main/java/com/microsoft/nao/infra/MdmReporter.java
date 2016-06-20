package com.microsoft.nao.infra;

import com.codahale.metrics.*;
import org.apache.spark.SparkEnv;

import java.net.InetAddress;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MdmReporter extends ScheduledReporter {

    private static final String headSourceNameRegex = "^(.*\\.StreamingMetrics";
    private static final String metricNameGroupName = "MetricName";
    private static final String tailSourceNameRegex = String.format("[^.]+)\\.(?<%s>.+)$", metricNameGroupName);
    private static final String metricNameSplitSeperator = "\\.";

    /**
     * Returns a new {@link Builder} for {@link MdmReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link MdmReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link MdmReporter} instances.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String monitoringAccount;
        private String metricNamespace;
        private String extraSourceNameRegex;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;

            this.monitoringAccount = "socbj";
            String environmentName = System.getenv("environment");
            if (environmentName != null && !environmentName.equals("")) {
                this.metricNamespace = "SparkMetrics/v2/" + environmentName;
            }
            else {
                this.metricNamespace = "SparkMetrics/v2/_default";
            }

            this.extraSourceNameRegex = "";
        }

        public Builder overrideMonitoringAccount(String monitoringAccount) {
            if (monitoringAccount != null && !monitoringAccount.equals("")) {
                this.monitoringAccount = monitoringAccount;
            }

            return this;
        }

        public Builder overrideMetricNamespace(String metricNamespace) {
            if (metricNamespace != null && !metricNamespace.equals("")) {
                this.metricNamespace = metricNamespace;
            }

            return this;
        }

        public Builder addExtraSourceNameRegex(String extraSourceNameRegex) {
            if (extraSourceNameRegex != null && !extraSourceNameRegex.equals("")) {
                this.extraSourceNameRegex = this.extraSourceNameRegex + "|" + extraSourceNameRegex;
            }

            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link MdmReporter} with the given properties.
         *
         * @return a {@link MdmReporter}
         */
        public MdmReporter build() {
            if (this.extraSourceNameRegex != null && this.extraSourceNameRegex.equals("")) {
                this.extraSourceNameRegex = headSourceNameRegex + "|" + this.extraSourceNameRegex + "|" + tailSourceNameRegex;
            }
            else {
                this.extraSourceNameRegex = headSourceNameRegex + "|" + tailSourceNameRegex;
            }

            return new MdmReporter(
                    this.registry,
                    this.monitoringAccount,
                    this.metricNamespace,
                    this.extraSourceNameRegex,
                    rateUnit,
                    durationUnit,
                    filter);
        }
    }

    private final String monitoringAccount;
    private final String metricNamespace;
    private final String sourceNameRegexString;

    private final String ParseFailureMetricName = "ParseFailureSparkMetrics";

    private MdmReporter(
            MetricRegistry registry,
            String monitoringAccount,
            String metricNamespace,
            String sourceNameRegexString,
            TimeUnit rateUnit,
            TimeUnit durationUnit,
            MetricFilter filter) {
        super(registry, "mdm-reporter", filter, rateUnit, durationUnit);

        this.monitoringAccount = monitoringAccount;
        this.metricNamespace = metricNamespace;
        this.sourceNameRegexString = sourceNameRegexString;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        if (!gauges.isEmpty()) {
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                printGauge(entry);
            }
        }
    }

    private void printGauge(Map.Entry<String, Gauge> entry) {
        String entryName = entry.getKey();
        String[] sections = entryName.split(metricNameSplitSeperator);

        MeasureMetric5D failureMetric = MeasureMetric5D.create(
                this.monitoringAccount,
                this.metricNamespace,
                ParseFailureMetricName,
                "Name",
                "SourceNameRegex",
                "RemainingString",
                "ApplicationId",
                "RootCause");

        if (sections.length < 3) {
            if (failureMetric != null) {
                failureMetric.LogValue(1, entryName, "", "", "", "InsufficientSectionLength");
            }

            return;
        }

        String machineName;
        try {
            machineName = InetAddress.getLocalHost().getHostName();
        }
        catch (Exception e) {
            machineName = "";
        }

        String applicationId = sections[0];
        String executorId = sections[1];
        String sourceName = "";
        String metricName = "";

        StringBuilder remainingStringBuilder = new StringBuilder();
        String joiner = "";
        for (int i = 2; i < sections.length; ++i) {
            remainingStringBuilder.append(joiner + sections[i]);
            joiner = ".";
        }

        String remainingString = remainingStringBuilder.toString();
        try {
            Pattern sourceNameRegex = Pattern.compile(this.sourceNameRegexString);
            Matcher m = sourceNameRegex.matcher(remainingString);
            if (m.find()) {
                sourceName = m.group(1);
                metricName = m.group(metricNameGroupName);
            } else {
                if (failureMetric != null) {
                    failureMetric.LogValue(1, entryName, this.sourceNameRegexString, remainingString, applicationId, "MatchFailure");
                }

                return;
            }
        }
        catch (Exception e) {
            if (failureMetric != null) {
                failureMetric.LogValue(1, entryName, this.sourceNameRegexString, remainingString, applicationId, "MatchException");
            }

            return;
        }

        MeasureMetric6D gaugeMetric = MeasureMetric6D.create(
                this.monitoringAccount,
                this.metricNamespace,
                metricName,
                "MachineName",
                "Type",
                "ApplicationId",
                "ExecutorId",
                "SourceName",
                "ApplicationName");
        long value;
        String applicationName = "";
        try {
            applicationName = SparkEnv.get().conf().get("spark.app.name", "");
            value = Long.parseLong(entry.getValue().getValue().toString());
        }
        catch (Exception e) {
            if (failureMetric != null) {
                failureMetric.LogValue(1, entryName, this.sourceNameRegexString, remainingString, applicationId, e.toString());
            }

            return;
        }

        gaugeMetric.LogValue(value, machineName, "Gauge", applicationId, executorId, sourceName, applicationName);
    }
}
