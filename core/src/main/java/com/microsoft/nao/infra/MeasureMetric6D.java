package com.microsoft.nao.infra;

import com.sun.jna.ptr.PointerByReference;

public class MeasureMetric6D {
    private PointerByReference hMetric;

    private MeasureMetric6D(PointerByReference hMetric) {
        this.hMetric = hMetric;
    }

    public static MeasureMetric6D create(
            String monitoringAccount,
            String metricNamespace,
            String metricName,
            String dimensionName1,
            String dimensionName2,
            String dimensionName3,
            String dimensionName4,
            String dimensionName5,
            String dimensionName6) {
        return create(monitoringAccount, metricNamespace, metricName, dimensionName1, dimensionName2, dimensionName3, dimensionName4, dimensionName5, dimensionName6, false);
    }

    public static MeasureMetric6D create(
            String monitoringAccount,
            String metricNamespace,
            String metricName,
            String dimensionName1,
            String dimensionName2,
            String dimensionName3,
            String dimensionName4,
            String dimensionName5,
            String dimensionName6,
            boolean addDefaultDimension) {
        PointerByReference hMetric = new PointerByReference();
        String[] dimensions = new String[6];
        dimensions[0] = dimensionName1;
        dimensions[1] = dimensionName2;
        dimensions[2] = dimensionName3;
        dimensions[3] = dimensionName4;
        dimensions[4] = dimensionName5;
        dimensions[5] = dimensionName6;
        long rc = IfxMetricsInterface.INSTANCE.CreateIfxMeasureMetric(
                hMetric,
                monitoringAccount,
                metricNamespace,
                metricName,
                6,
                dimensions,
                addDefaultDimension);
        if (rc >= 0) {
            return new MeasureMetric6D(hMetric);
        }

        return null;
    }

    public long LogValue(
            long rawData,
            String dimensionValue1,
            String dimensionValue2,
            String dimensionValue3,
            String dimensionValue4,
            String dimensionValue5,
            String dimensionValue6) {
        String[] dimensions = new String[6];
        dimensions[0] = dimensionValue1;
        dimensions[1] = dimensionValue2;
        dimensions[2] = dimensionValue3;
        dimensions[3] = dimensionValue4;
        dimensions[4] = dimensionValue5;
        dimensions[5] = dimensionValue6;

        return IfxMetricsInterface.INSTANCE.SetIfxMeasureMetric(this.hMetric.getValue(), rawData, 6, dimensions);
    }

    public long LogValue(
            long timestampUtc,
            long rawData,
            String dimensionValue1,
            String dimensionValue2,
            String dimensionValue3,
            String dimensionValue4,
            String dimensionValue5,
            String dimensionValue6) {
        String[] dimensions = new String[6];
        dimensions[0] = dimensionValue1;
        dimensions[1] = dimensionValue2;
        dimensions[2] = dimensionValue3;
        dimensions[3] = dimensionValue4;
        dimensions[4] = dimensionValue5;
        dimensions[5] = dimensionValue6;

        return IfxMetricsInterface.INSTANCE.SetIfxMeasureMetricWithTimestamp(this.hMetric.getValue(), timestampUtc, rawData, 6, dimensions);
    }
}
