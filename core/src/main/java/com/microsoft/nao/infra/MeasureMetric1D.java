package com.microsoft.nao.infra;

import com.sun.jna.ptr.PointerByReference;

public class MeasureMetric1D {
    private PointerByReference hMetric;

    private MeasureMetric1D(PointerByReference hMetric) {
        this.hMetric = hMetric;
    }

    public static MeasureMetric1D create(
            String monitoringAccount,
            String metricNamespace,
            String metricName,
            String dimensionName1) {
        return create(monitoringAccount, metricNamespace, metricName, dimensionName1, false);
    }

    public static MeasureMetric1D create(
            String monitoringAccount,
            String metricNamespace,
            String metricName,
            String dimensionName1,
            boolean addDefaultDimension) {
        PointerByReference hMetric = new PointerByReference();
        String[] dimensions = new String[1];
        dimensions[0] = dimensionName1;
        long rc = IfxMetricsInterface.INSTANCE.CreateIfxMeasureMetric(
                hMetric,
                monitoringAccount,
                metricNamespace,
                metricName,
                1,
                dimensions,
                addDefaultDimension);
        System.out.println(rc);
        if (rc >= 0) {
            return new MeasureMetric1D(hMetric);
        }

        return null;
    }

    public long LogValue(
            long rawData,
            String dimensionValue1) {
        String[] dimensions = new String[1];
        dimensions[0] = dimensionValue1;

        return IfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 1, dimensions);
    }

    public long LogValue(
            long timestampUtc,
            long rawData,
            String dimensionValue1) {
        String[] dimensions = new String[1];
        dimensions[0] = dimensionValue1;

        return IfxMetricsInterface.INSTANCE.SetIfxMeasureMetricWithTimestamp(this.hMetric.getValue(), timestampUtc, rawData, 1, dimensions);
    }
}
