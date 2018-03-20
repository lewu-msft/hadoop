package org.apache.hadoop.fs.adl.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.Metric;
import com.microsoft.azure.datalake.store.MetricRegistry;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * A metrics source for the ADL file system to track all the metrics we care
 * about for getting a clear picture of the performance/reliability/interaction
 * of the Hadoop cluster with Azure Data Lake Store.
 * Derived from S3AMetrics
 */
@Metrics(about="Metrics for ADLS", context="ADLSFileSystem")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AdlFileSystemInstrumentation implements Closeable, MetricsSource {
    static final Logger LOG = LoggerFactory.getLogger(AdlFileSystemInstrumentation.class);
    private static final String METRICS_SOURCE_BASENAME = "AdlMetrics";

    /**
     * {@value #METRICS_SYSTEM_NAME} The name of the adl-specific metrics
     * system instance used for adl metrics.
     */
    public static final String METRICS_SYSTEM_NAME = "adl-file-system";

    /**
     * {@value #METRIC_TAG_BUCKET} The name of a field added to metrics records
     * that indicates the hostname portion of the FS URL.
     */
    public static final String METRIC_TAG_BUCKET = "bucket";

    private String metricsSourceName;

    private ADLStoreClient adlStoreClient;

    // metricsSystemLock must be used to synchronize modifications to
    // metricsSystem and the following counters.
    private static Object metricsSystemLock = new Object();
    private static MetricsSystem metricsSystem = null;
    private static int metricsSourceNameCounter = 0;
    private static int metricsSourceActiveCounter = 0;

    private final MetricsRegistry registry = new MetricsRegistry("adlFileSystem").setContext("adlFileSystem");

    public AdlFileSystemInstrumentation(URI uri, ADLStoreClient adlStoreClient) {
        UUID fileSystemInstanceId = UUID.randomUUID();
        registry.tag("adlFileSystemId",
                "A unique identifier for the file ",
                fileSystemInstanceId.toString());
        registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", uri.getHost());
        registerAsMetricsSource(uri);
        this.adlStoreClient = adlStoreClient;
    }

    @VisibleForTesting
    public MetricsSystem getMetricsSystem() {
        synchronized (metricsSystemLock) {
            if (metricsSystem == null) {
                metricsSystem = new MetricsSystemImpl();
                metricsSystem.init(METRICS_SYSTEM_NAME);
            }
        }
        return metricsSystem;
    }

    /**
     * Get the metrics registry.
     * @return the registry
     */
    public MetricsRegistry getRegistry() {
        return registry;
    }

    /**
     * Register this instance as a metrics source.
     * @param uri
     */
    private void registerAsMetricsSource(URI uri) {
        int number;
        synchronized(metricsSystemLock) {
            getMetricsSystem();

            metricsSourceActiveCounter++;
            number = ++metricsSourceNameCounter;
        }
        String msName = METRICS_SOURCE_BASENAME + number;
        if (number > 1) {
            msName = msName + number;
        }
        metricsSourceName = msName + "-" + uri.getHost();
        metricsSystem.register(metricsSourceName, "", this);
    }

    /**
     * Increment a specific counter.
     * No-op if not defined.
     * @param op operation
     * @param count increment value
     */
    public void incrementCounter(Statistic op, long count) {
        MutableCounterLong counter = lookupCounter(op.getSymbol());
        if (counter != null) {
            counter.incr(count);
        }
        LOG.info(op.getSymbol() + " metrics counter incremented!");
    }

    /**
     * Lookup a counter by name. Return null if it is not known.
     * @param name counter name
     * @return the counter
     * @throws IllegalStateException if the metric is not a counter
     */
    private MutableCounterLong lookupCounter(String name) {
        MutableMetric metric = lookupMetric(name);
        if (metric == null) {
            return null;
        }
        if (!(metric instanceof MutableCounterLong)) {
            throw new IllegalStateException("Metric " + name
                    + " is not a MutableCounterLong: " + metric);
        }
        LOG.info("lookup counter log for " + name);
        return (MutableCounterLong) metric;
    }

    /**
     * Look up a metric from both the registered set and the lighter weight
     * stream entries.
     * @param name metric name
     * @return the metric or null
     */
    public MutableMetric lookupMetric(String name) {
        MutableMetric metric = getRegistry().get(name);
        return metric;
    }

    public void close() {
        synchronized (metricsSystemLock) {
            metricsSystem.unregisterSource(metricsSourceName);
            int activeSources = --metricsSourceActiveCounter;
            if (activeSources == 0) {
                metricsSystem.publishMetricsNow();
                metricsSystem.shutdown();
                metricsSystem = null;
            }
        }
    }

    @Override
    public void getMetrics(MetricsCollector builder, boolean all) {
        dumpAllMetrics();
        registry.snapshot(builder.addRecord(registry.info().name()), true);
    }

    public void dumpAllMetrics() {
        List<Metric> metricList = adlStoreClient.getMetricList();
        for (Metric metric : metricList) {
            registry.newCounter(metric.getName(), metric.getDescription(), metric.getTotal());
            LOG.info("metricName: " + metric.getName() + " with metricTotal of: " + metric.getTotal());
        }
    }
}
