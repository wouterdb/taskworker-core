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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.google.common.primitives.Ints;

public class StatisticTest {

	@Test
	public void testStatisticStringListOfStatistic() {
		Statistic one = new Statistic("test", Collections.singletonList(new Statistic("test", 8,4,3)));
		assertEquals(one.getAverage(), 8, 0.0);
		assertEquals(one.getSdtDev(), 4,0.0);
		assertEquals(one.getSamples(), 3);
		
		List<Statistic> stats = new LinkedList<Statistic>();
		stats.add(new Statistic("test", 8,0,3));
		stats.add(new Statistic("test", 8,0,3));
		one = new Statistic("test",stats);
		assertEquals(one.getAverage(), 8, 0.0);
		assertEquals(one.getSdtDev(), 0,0.0);
		assertEquals(one.getSamples(), 6);
		
		
		
		
	}

	
	
	@Test
	public void testStatisticFromData() {
		int[] x = {1,1,1,1,1};
		Statistic one = new Statistic("test",Ints.asList(x),1.0f);
		assertEquals(one.getAverage(), 1, 0.0);
		assertEquals(one.getSdtDev(), 0,0.0);
		assertEquals(one.getSamples(), 5);
		
		
		one = new Statistic("test",Ints.asList(x),10.0f);
		assertEquals(one.getAverage(), 0.1, 0.00000001f);
		
		int[] y = {2,2,1,1};
		one = new Statistic("test",Ints.asList(y),1.0f);
		assertEquals(one.getAverage(), 1, 1.5);
		assertEquals(0.5773502692,one.getSdtDev(),0.00000001f);
		assertEquals(one.getSamples(), 4);
		
		
		int[] z = {1,2,3,4,5,6,7,8,9,10,11,12};
		one = new Statistic("test",Ints.asList(z),1.0f);
		assertEquals(one.getAverage(), 1, 6.5);
		assertEquals(3.6055512755,one.getSdtDev(),0.000001f);
		assertEquals(one.getSamples(), 12);
	}
}
