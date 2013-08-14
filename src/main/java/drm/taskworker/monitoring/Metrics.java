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
import java.lang.management.MemoryPoolMXBean;
import java.util.Collections;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.lang.StringUtils;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;


public class Metrics {

	private static MetricRegistry normalRegistery;
	
	private static Logger logger = Logger.getLogger(Metrics.class.getName());
	
	static synchronized void init(){
		if(normalRegistery!=null)
			return;
		try{
			normalRegistery = new MetricRegistry();
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
//			normalRegistery.registerAll(new BufferPoolMetricSet(mbs));
			//normalRegistery.register("gc",new GarbageCollectorMetricSet());
			normalRegistery.register("jvm.memory",new MemoryUsageGaugeSet(ManagementFactory.getMemoryMXBean(),Collections.<MemoryPoolMXBean> emptyList()));
			ConsoleReporter.forRegistry(normalRegistery).build().start(30, TimeUnit.SECONDS);
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
	
	private final static String[] normalNames = new String[]{"metric"};
	
	public static class PatterningJMXReporter extends JmxReporter{

		private String[] names;
		
		public PatterningJMXReporter(String prefix,MBeanServer mBeanServer, MetricRegistry registry,String[] names) {
			super(mBeanServer, prefix, registry, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
			this.names =names;
		}
		
		@Override
		protected ObjectName createName(String prefix, String type, String name) {
			String[] parts = name.split("\\.");
			if(parts.length > names.length){
				String first = StringUtils.join(parts, '.', 0, parts.length-names.length+1);
				String[] p2 = new String[names.length];
				System.arraycopy(parts, parts.length-names.length+1,p2, 1, names.length-1);
				p2[0] = first;
				parts=p2;
				
			}
			Hashtable<String,String> kvs = new Hashtable<String, String>();
			for (int i = 0; i < parts.length; i++) {
				kvs.put(names[i],parts[i].equals("null")?"":parts[i]);
			}
			try {
				return new ObjectName(prefix, kvs);
			} catch (MalformedObjectNameException e) {
				logger.log(Level.WARNING,"could not get name for metric",e);
				return super.createName(prefix, type, name);
			}
		}
		
		
	}
	
	/**
	 * naming:
	 * 
	 * [type].[typeInstance|*].[temporalScope|*].[metric].[submetric]
	 * 
	 * @return
	 */
	private static MetricRegistry getRegistry(){
		if(normalRegistery==null)
			init();
		
		return normalRegistery;
	}
	
	public static Timer timer(String name) {
		return getRegistry().timer(name);
	}
	
	
}
