/**
 *
 *     Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 *     Administrative Contact: dnet-project-office@cs.kuleuven.be
 *     Technical Contact: bart.vanbrabant@cs.kuleuven.be
 */
//taken from metrics-core

package drm.taskworker.monitoring;


import javax.management.*;

import com.codahale.metrics.*;


import java.lang.management.ManagementFactory;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A reporter which listens for new metrics and exposes them as namespaced MBeans.
 */
public class JmxReporter {
    /**
     * Returns a new {@link Builder} for {@link JmxReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link JmxReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link CsvReporter} instances. Defaults to using the default MBean server and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter = MetricFilter.ALL;
        private String domain;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.domain = "metrics";
        }

        /**
         * Register MBeans with the given {@link MBeanServer}.
         *
         * @param mBeanServer     an {@link MBeanServer}
         * @return {@code this}
         */
        public Builder registerWith(MBeanServer mBeanServer) {
            this.mBeanServer = mBeanServer;
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

        public Builder inDomain(String domain) {
            this.domain = domain;
            return this;
        }

        /**
         * Builds a {@link JmxReporter} with the given properties.
         *
         * @return a {@link JmxReporter}
         */
        public JmxReporter build() {
            return new JmxReporter(mBeanServer, domain, registry, filter, rateUnit, durationUnit);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(JmxReporter.class.getName());

    // CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface MetricMBean {
        ObjectName objectName();
    }
    // CHECKSTYLE:ON


    private abstract static class AbstractBean implements MetricMBean {
        private final ObjectName objectName;

        AbstractBean(ObjectName objectName) {
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName() {
            return objectName;
        }
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface JmxGaugeMBean extends MetricMBean {
        Object getValue();
    }
    // CHECKSTYLE:ON

    private static class JmxGauge extends AbstractBean implements JmxGaugeMBean {
        private final Gauge<?> metric;

        private JmxGauge(Gauge<?> metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public Object getValue() {
            return metric.getValue();
        }
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface JmxCounterMBean extends MetricMBean {
        long getCount();
    }
    // CHECKSTYLE:ON

    private static class JmxCounter extends AbstractBean implements JmxCounterMBean {
        private final Counter metric;

        private JmxCounter(Counter metric, ObjectName objectName) {
            super(objectName);
            this.metric = metric;
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface JmxHistogramMBean extends MetricMBean {
        long getCount();

        long getMin();

        long getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();
    }
    // CHECKSTYLE:ON

    private static class JmxHistogram implements JmxHistogramMBean {
        private final ObjectName objectName;
        private final Histogram metric;

        private JmxHistogram(Histogram metric, ObjectName objectName) {
            this.metric = metric;
            this.objectName = objectName;
        }

        @Override
        public ObjectName objectName() {
            return objectName;
        }

        @Override
        public double get50thPercentile() {
            return metric.getSnapshot().getMedian();
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }

        @Override
        public long getMin() {
            return metric.getSnapshot().getMin();
        }

        @Override
        public long getMax() {
            return metric.getSnapshot().getMax();
        }

        @Override
        public double getMean() {
            return metric.getSnapshot().getMean();
        }

        @Override
        public double getStdDev() {
            return metric.getSnapshot().getStdDev();
        }

        @Override
        public double get75thPercentile() {
            return metric.getSnapshot().get75thPercentile();
        }

        @Override
        public double get95thPercentile() {
            return metric.getSnapshot().get95thPercentile();
        }

        @Override
        public double get98thPercentile() {
            return metric.getSnapshot().get98thPercentile();
        }

        @Override
        public double get99thPercentile() {
            return metric.getSnapshot().get99thPercentile();
        }

        @Override
        public double get999thPercentile() {
            return metric.getSnapshot().get999thPercentile();
        }

        @Override
        public long[] values() {
            return metric.getSnapshot().getValues();
        }
    }

    //CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface JmxMeterMBean extends MetricMBean {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }
    //CHECKSTYLE:ON

    private static class JmxMeter extends AbstractBean implements JmxMeterMBean {
        private final Metered metric;
        private final double rateFactor;
        private final String rateUnit;

        private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
            super(objectName);
            this.metric = metric;
            this.rateFactor = rateUnit.toSeconds(1);
            this.rateUnit = "events/" + calculateRateUnit(rateUnit);
        }

        @Override
        public long getCount() {
            return metric.getCount();
        }

        @Override
        public double getMeanRate() {
            return metric.getMeanRate() * rateFactor;
        }

        @Override
        public double getOneMinuteRate() {
            return metric.getOneMinuteRate() * rateFactor;
        }

        @Override
        public double getFiveMinuteRate() {
            return metric.getFiveMinuteRate() * rateFactor;
        }

        @Override
        public double getFifteenMinuteRate() {
            return metric.getFifteenMinuteRate() * rateFactor;
        }

        @Override
        public String getRateUnit() {
            return rateUnit;
        }

        private String calculateRateUnit(TimeUnit unit) {
            final String s = unit.toString().toLowerCase(Locale.US);
            return s.substring(0, s.length() - 1);
        }
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("UnusedDeclaration")
    public interface JmxTimerMBean extends JmxMeterMBean {
        double getMin();

        double getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();
        String getDurationUnit();
    }
    // CHECKSTYLE:ON

    static class JmxTimer extends JmxMeter implements JmxTimerMBean {
        private final Timer metric;
        private final double durationFactor;
        private final String durationUnit;

        private JmxTimer(Timer metric,
                         ObjectName objectName,
                         TimeUnit rateUnit,
                         TimeUnit durationUnit) {
            super(metric, objectName, rateUnit);
            this.metric = metric;
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
        }

        @Override
        public double get50thPercentile() {
            return metric.getSnapshot().getMedian() * durationFactor;
        }

        @Override
        public double getMin() {
            return metric.getSnapshot().getMin() * durationFactor;
        }

        @Override
        public double getMax() {
            return metric.getSnapshot().getMax() * durationFactor;
        }

        @Override
        public double getMean() {
            return metric.getSnapshot().getMean() * durationFactor;
        }

        @Override
        public double getStdDev() {
            return metric.getSnapshot().getStdDev() * durationFactor;
        }

        @Override
        public double get75thPercentile() {
            return metric.getSnapshot().get75thPercentile() * durationFactor;
        }

        @Override
        public double get95thPercentile() {
            return metric.getSnapshot().get95thPercentile() * durationFactor;
        }

        @Override
        public double get98thPercentile() {
            return metric.getSnapshot().get98thPercentile() * durationFactor;
        }

        @Override
        public double get99thPercentile() {
            return metric.getSnapshot().get99thPercentile() * durationFactor;
        }

        @Override
        public double get999thPercentile() {
            return metric.getSnapshot().get999thPercentile() * durationFactor;
        }

        @Override
        public long[] values() {
            return metric.getSnapshot().getValues();
        }

        @Override
        public String getDurationUnit() {
            return durationUnit;
        }
    }

    private class JmxListener implements MetricRegistryListener {
        private final String name;
        private final MBeanServer mBeanServer;
        private final MetricFilter filter;
        private final TimeUnit rateUnit;
        private final TimeUnit durationUnit;
        private final Set<ObjectName> registered;

        private JmxListener(MBeanServer mBeanServer,
                            String name,
                            MetricFilter filter,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit) {
            this.mBeanServer = mBeanServer;
            this.name = name;
            this.filter = filter;
            this.rateUnit = rateUnit;
            this.durationUnit = durationUnit;
            this.registered = new CopyOnWriteArraySet<ObjectName>();
        }

        @Override
        public void onGaugeAdded(String name, Gauge<?> gauge) {
            try {
                if (filter.matches(name, gauge)) {
                    final ObjectName objectName = createName("gauges", name);
                    mBeanServer.registerMBean(new JmxGauge(gauge, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                LOGGER.log(Level.FINER,"Unable to register gauge", e);
            } catch (JMException e) {
                LOGGER.log(Level.INFO,"Unable to register gauge", e);
            }
        }

        @Override
        public void onGaugeRemoved(String name) {
            try {
                final ObjectName objectName = createName("gauges", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
				LOGGER.log(Level.FINER,"Unable to unregister gauge", e);
            } catch (MBeanRegistrationException e) {
                LOGGER.log(Level.INFO,"Unable to unregister gauge", e);
            }
        }

        @Override
        public void onCounterAdded(String name, Counter counter) {
            try {
                if (filter.matches(name, counter)) {
                    final ObjectName objectName = createName("counters", name);
                    mBeanServer.registerMBean(new JmxCounter(counter, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                LOGGER.log(Level.FINER,"Unable to register counter", e);
            } catch (JMException e) {
                LOGGER.log(Level.INFO,"Unable to register counter", e);
            }
        }

        @Override
        public void onCounterRemoved(String name) {
            try {
                final ObjectName objectName = createName("counters", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                LOGGER.log(Level.FINER,"Unable to unregister counter", e);
            } catch (MBeanRegistrationException e) {
                LOGGER.log(Level.INFO,"Unable to unregister counter", e);
            }
        }

        @Override
        public void onHistogramAdded(String name, Histogram histogram) {
            try {
                if (filter.matches(name, histogram)) {
                    final ObjectName objectName = createName("histograms", name);
                    mBeanServer.registerMBean(new JmxHistogram(histogram, objectName), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                LOGGER.log(Level.FINER,"Unable to register histogram", e);
            } catch (JMException e) {
                LOGGER.log(Level.INFO,"Unable to register histogram", e);
            }
        }

        @Override
        public void onHistogramRemoved(String name) {
            try {
                final ObjectName objectName = createName("histograms", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                LOGGER.log(Level.FINER,"Unable to unregister histogram", e);
            } catch (MBeanRegistrationException e) {
                LOGGER.log(Level.INFO,"Unable to unregister histogram", e);
            }
        }

        @Override
        public void onMeterAdded(String name, Meter meter) {
            try {
                if (filter.matches(name, meter)) {
                    final ObjectName objectName = createName("meters", name);
                    mBeanServer.registerMBean(new JmxMeter(meter, objectName, rateUnit), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                LOGGER.log(Level.FINER,"Unable to register meter", e);
            } catch (JMException e) {
                LOGGER.log(Level.INFO,"Unable to register meter", e);
            }
        }

        @Override
        public void onMeterRemoved(String name) {
            try {
                final ObjectName objectName = createName("meters", name);
                mBeanServer.unregisterMBean(objectName);
                registered.remove(objectName);
            } catch (InstanceNotFoundException e) {
                LOGGER.log(Level.FINER,"Unable to unregister meter", e);
            } catch (MBeanRegistrationException e) {
                LOGGER.log(Level.INFO,"Unable to unregister meter", e);
            }
        }

        @Override
        public void onTimerAdded(String name, Timer timer) {
            try {
                if (filter.matches(name, timer)) {
                    final ObjectName objectName = createName("timers", name);
                    mBeanServer.registerMBean(new JmxTimer(timer, objectName, rateUnit, durationUnit), objectName);
                    registered.add(objectName);
                }
            } catch (InstanceAlreadyExistsException e) {
                LOGGER.log(Level.FINER,"Unable to register timer", e);
            } catch (JMException e) {
                LOGGER.log(Level.INFO,"Unable to register timer", e);
            }
        }

        @Override
        public void onTimerRemoved(String name) {
            try {
                final ObjectName objectName = createName("timers", name);
                mBeanServer.unregisterMBean(objectName);
                registered.add(objectName);
            } catch (InstanceNotFoundException e) {
                LOGGER.log(Level.FINER,"Unable to unregister timer", e);
            } catch (MBeanRegistrationException e) {
                LOGGER.log(Level.INFO,"Unable to unregister timer", e);
            }
        }

        private ObjectName createName(String type, String name) {
        	return JmxReporter.this.createName(this.name,type, name);
        }

        void unregisterAll() {
            for (ObjectName name : registered) {
                try {
                    mBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException e) {
                    LOGGER.log(Level.FINER,"Unable to unregister metric", e);
                } catch (MBeanRegistrationException e) {
                    LOGGER.log(Level.INFO,"Unable to unregister metric", e);
                }
            }
            registered.clear();
        }
    }

    protected ObjectName createName(String prefix,String type, String name) {
    	 try {
             return new ObjectName(prefix, "name", name);
         } catch (MalformedObjectNameException e) {
             try {
                 return new ObjectName(prefix, "name", ObjectName.quote(name));
             } catch (MalformedObjectNameException e1) {
                 LOGGER.log(Level.INFO,"Unable to register " + type + " " + name, e1);
                 throw new RuntimeException(e1);
             }
         }
    }
    
    
    private final MetricRegistry registry;
    private final JmxListener listener;

    public JmxReporter(MBeanServer mBeanServer,
                        String domain,
                        MetricRegistry registry,
                        MetricFilter filter,
                        TimeUnit rateUnit,
                        TimeUnit durationUnit) {
        this.registry = registry;
        this.listener = new JmxListener(mBeanServer, domain, filter, rateUnit, durationUnit);
    }

    /**
     * Starts the reporter.
     */
    public void start() {
        registry.addListener(listener);
    }

    /**
     * Stops the reporter.
     */
    public void stop() {
        registry.removeListener(listener);
        listener.unregisterAll();
    }
}