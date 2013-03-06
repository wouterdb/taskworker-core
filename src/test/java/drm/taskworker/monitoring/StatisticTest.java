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

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import drm.taskworker.monitoring.Statistic;

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

}
