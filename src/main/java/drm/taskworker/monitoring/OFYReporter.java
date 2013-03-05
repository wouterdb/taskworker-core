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
package drm.taskworker.monitoring;

import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.ThreadManager;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import drm.taskworker.Entities;

public class OFYReporter extends AbstractPollingReporter implements
		MetricProcessor<Snapshot> {

	private static Logger logger = Logger.getLogger(OFYReporter.class
			.getCanonicalName());

	public OFYReporter() {
		super(Metrics.defaultRegistry(), "OFYReporter");
	}

	@Override
	public void processMeter(MetricName name, Metered meter, Snapshot context)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void processCounter(MetricName name, Counter counter,
			Snapshot context) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void processHistogram(MetricName name, Histogram histogram,
			Snapshot context) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void processTimer(MetricName name, Timer timer, Snapshot context)
			throws Exception {

		context.getStats().add(
				new Statistic(name.getName(), timer.mean(), timer.stdDev(),
						timer.count()));

	}

	@Override
	public void processGauge(MetricName name, Gauge<?> gauge, Snapshot context)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void run() {
		Snapshot s = new Snapshot(new Date());

		for (Entry<MetricName, Metric> m : getMetricsRegistry().allMetrics()
				.entrySet()) {
			try {
				m.getValue().processWith(this, m.getKey(), s);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "should not occur", e);
			}
		}
		Entities.ofy().save().entity(s);

	}

	ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1,
			ThreadManager.backgroundThreadFactory());

	@Override
	public void start(long period, TimeUnit unit) {
		executor.scheduleAtFixedRate(this, period, period, unit);
	}

}
