package org.apache.hadoop.fs.adl.metrics;

import com.google.common.annotations.VisibleForTesting;
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

import java.io.Closeable;
import java.net.URI;
import java.util.UUID;

import static org.apache.hadoop.fs.adl.metrics.Statistic.*;

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

    // metricsSystemLock must be used to synchronize modifications to
    // metricsSystem and the following counters.
    private static Object metricsSystemLock = new Object();
    private static MetricsSystem metricsSystem = null;
    private static int metricsSourceNameCounter = 0;
    private static int metricsSourceActiveCounter = 0;

    private final MetricsRegistry registry = new MetricsRegistry("adlFileSystem").setContext("adlFileSystem");

    private static final Statistic[] COUNTERS_TO_CREATE = {
            OPEN,
            OPEN_LATENCY,
            GETFILESTATUS,
            GETFILESTATUS_LATENCY,
            MSGETFILESTATUS,
            MSGETFILESTATUS_LATENCY
    };

    public AdlFileSystemInstrumentation(URI uri) {
        UUID fileSystemInstanceId = UUID.randomUUID();
        registry.tag("adlFileSystemId",
                "A unique identifier for the file ",
                fileSystemInstanceId.toString());
        registry.tag(METRIC_TAG_BUCKET, "Hostname from the FS URL", uri.getHost());

        for (Statistic statistic : COUNTERS_TO_CREATE) {
            counter(statistic);
        }
        registerAsMetricsSource(uri);
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
     * Create a counter in the registry.
     * @param name counter name
     * @param desc counter description
     * @return a new counter
     */
    protected final MutableCounterLong counter(String name, String desc) {
        return registry.newCounter(name, desc, 0L);
    }

    /**
     * Create a counter in the registry.
     * @param op statistic to count
     * @return a new counter
     */
    protected final MutableCounterLong counter(Statistic op) {
        return counter(op.getSymbol(), op.getDescription());
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
        registry.snapshot(builder.addRecord(registry.info().name()), true);
    }
}
