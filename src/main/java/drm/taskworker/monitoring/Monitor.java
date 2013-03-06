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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;

public class Monitor implements IMonitor,MetricProcessor<Set<Statistic>>{

	private static Logger logger = Logger.getLogger(Monitor.class
			.getCanonicalName());

	/*
	 * @Override public Set<Statistic> getStats(Date start, Date end) {
	 * 
	 * Map<String,List<Statistic>> snapshots = new HashMap<String,
	 * List<Statistic>>();
	 * 
	 * for(Snapshot s:ofy().load().type(Snapshot.class).filter("timestamp >=",
	 * start.getTime()).filter("timestamp <",end.getTime()).iterable()){
	 * for(Statistic stat:s.getStats()){ List<Statistic> mine =
	 * snapshots.get(stat.getName()); if(mine==null){ mine = new
	 * LinkedList<Statistic>(); snapshots.put(stat.getName(), mine); }
	 * mine.add(stat); } }
	 * 
	 * Set<Statistic> out = new HashSet<Statistic>(); for(Map.Entry<String,
	 * List<Statistic>> entry:snapshots.entrySet()){ out.add(new
	 * Statistic(entry.getKey(), entry.getValue())); } return out; }
	 */
	@Override
	public Set<Statistic> getStats() {
		Set<Statistic> s = new HashSet<Statistic>();
	
		for (Entry<MetricName, Metric> m : Metrics.defaultRegistry()
				.allMetrics().entrySet()) {

			try {
				m.getValue().processWith(this, m.getKey(), s);
			} catch (Exception e) {
				logger.log(Level.SEVERE, "should not occur", e);
			}

		}
		
		return s;
	}

	@Override
	public void processMeter(MetricName name, Metered meter,
			Set<Statistic> context) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processCounter(MetricName name, Counter counter,
			Set<Statistic> context) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processHistogram(MetricName name, Histogram histogram,
			Set<Statistic> context) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processTimer(MetricName name, Timer timer,
			Set<Statistic> context) throws Exception {
		context.add(
				new Statistic(name.getName(), timer.mean(), timer.stdDev(),
						timer.count()));
		
	}

	@Override
	public void processGauge(MetricName name, Gauge<?> gauge,
			Set<Statistic> context) throws Exception {
		// TODO Auto-generated method stub
		
	}
}
