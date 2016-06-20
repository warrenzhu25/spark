package com.microsoft.nao.infra;

import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IfxMetricsSendTest {

    private static final String headSourceNameRegex = "^(.*\\.StreamingMetrics";
    private static final String metricNameGroupName = "MetricName";
    private static final String tailSourceNameRegex = String.format("[^.]+)\\.(?<%s>.+)$", metricNameGroupName);

    public static void main(String[] args) {
        MeasureMetric1D metric = MeasureMetric1D.create("socbj", "SparkMetrics-Test", "testMetric", "MachineName");

        System.out.println("metric created. ");
        String machineName;
        try {
            machineName = InetAddress.getLocalHost().getHostName();
        }
        catch (Exception e) {
            machineName = "";
        }

        System.out.println(machineName);
        System.out.println(metric.LogValue(1, machineName));

        String entryName = "application_1465943342094_0013.driver.DAGScheduler.job.activeJobs";
        String[] sections = entryName.split("\\.");

        if (sections.length < 3) {
            System.out.println("InsufficientSectionLength");

            return;
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
            Pattern sourceNameRegex = Pattern.compile(headSourceNameRegex + "|" + tailSourceNameRegex);
            Matcher m = sourceNameRegex.matcher(remainingString);
            if (m.find()) {
                sourceName = m.group(0);
                metricName = m.group(metricNameGroupName);
            } else {
                System.out.println("MatchFailure");

                return;
            }
        }
        catch (Exception e) {
            System.out.println("MatchException");

            return;
        }

        System.out.println(applicationId);
        System.out.println(executorId);
        System.out.println(sourceName);
        System.out.println(metricName);
    }
}
