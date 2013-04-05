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

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;

import com.google.appengine.api.log.LogService.LogLevel;
import com.yammer.metrics.JmxReporter;
import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.jvm.BufferPoolMetricSet;
import com.yammer.metrics.jvm.GarbageCollectorMetricSet;
import com.yammer.metrics.jvm.MemoryUsageGaugeSet;

public class Metrics {

	private static MetricRegistry normalRegistery ;
	private static MetricRegistry tempRegistery;
	
	static synchronized void init(){
		if(normalRegistery!=null)
			return;
		try{
			normalRegistery = new MetricRegistry();
			tempRegistery = new DecayingRegistry(10,TimeUnit.MINUTES);
//			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
//			normalRegistery.registerAll(new BufferPoolMetricSet(mbs));
//			normalRegistery.registerAll(new GarbageCollectorMetricSet());
//			normalRegistery.registerAll(new MemoryUsageGaugeSet());
			JmxReporter.forRegistry(normalRegistery).build().start();
			JmxReporter.forRegistry(tempRegistery).inDomain("metrics-details").build().start();
		}catch (RuntimeException e) {
			Logger.getLogger(Metrics.class.getName()).log(Level.SEVERE,"metrics failed to load",e);
			throw e;
		}
		
	}
	
	
	/**
	 * naming:
	 * 
	 * [type].[typeInstance|null].[metric].[submetric]
	 * 
	 * @return
	 */
	public static MetricRegistry getNormalRegistry(){
		if(normalRegistery==null)
			init();
		return normalRegistery;
	}
	
	
	/**
	 * naming:
	 * 
	 * [type].[typeInstance|*].[temporalScope|*].[metric].[submetric]
	 * 
	 * @return
	 */
	public static MetricRegistry getTempRegistry(){
		if(normalRegistery==null)
			init();
		return tempRegistery;
	}
	
}
