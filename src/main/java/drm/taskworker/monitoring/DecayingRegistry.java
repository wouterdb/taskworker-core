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

import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.jboss.as.ejb3.timerservice.schedule.attribute.Second;
import org.jboss.util.collection.ConcurrentSet;

import com.yammer.metrics.Metric;
import com.yammer.metrics.MetricRegistry;

public class DecayingRegistry extends MetricRegistry {

	public class MyConcMap extends ConcurrentHashMap<String, Metric> implements
			ConcurrentMap<String, Metric> {

		@Override
		public Metric get(Object key) {
			lastSeen.add((String) key);
			return super.get(key);
		}

	}

	public class DecayTask extends TimerTask {

		@Override
		public void run() {
			Set<String> ls = lastSeen;
			lastSeen = new ConcurrentSet<String>();

			for (String metr : getNames()) {
				if (!ls.contains(metr))
					remove(metr);
			}
			// System.out.println(getNames().size());
		}

	}

	private ConcurrentSet<String> lastSeen = new ConcurrentSet<String>();

	public DecayingRegistry(int interval, TimeUnit unit) {
		Timer t = new Timer("metrics-reaper");
		t.scheduleAtFixedRate(new DecayTask(), unit.toMillis(interval),
				unit.toMillis(interval));
	}

	@Override
	protected ConcurrentMap<String, Metric> buildMap() {
		return new MyConcMap();
	}

}
