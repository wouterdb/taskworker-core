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
package drm.taskworker.schedule;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

public class WeightedRoundRobinTest {

	String[] evens = {"1","2"};
	float[] even = {1,1};
	float[] evenx = {0.5f,1.0f};
	
	String[] even2s = {"1","2","3","4","5"};
	float[] even2 = {1,1,1,1,1};
	float[] even2x = {0.2f,0.4f,0.6f,0.8f,1.0f};
	
	String[] bigones = {"1","2","3","4","5"};
	float[] bigone = {1,1,4,1,1};
	float[] bigonex = {0.125f,0.25f,0.75f,0.875f,1};
	
	@Test
	public void testGetBorders() {
		WeightedRoundRobin wrr = new WeightedRoundRobin(evens, even);
		assertArrayEquals(wrr.getBorders(), evenx,0.0f);
		
		wrr = new WeightedRoundRobin(even2s, even2);
		assertArrayEquals(wrr.getBorders(), even2x,0.0f);
		
		wrr = new WeightedRoundRobin(bigones, bigone);
		assertArrayEquals(wrr.getBorders(), bigonex,0.0f);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testGetBorders2() {
		new WeightedRoundRobin(evens, even2);
	}

	
	@Test
	public void testGetNext() {
		testgetNext(even);
		testgetNext(even2);
		testgetNext(bigone);
	}

	public static final int repeats = 100000;
	
	private void testgetNext(float[] probs) {
		String[] names = new String[probs.length];
		int[] counts = new int[probs.length];
		for (int i = 0; i < names.length; i++) {
			names[i] = ""+i;
		}
		
		WeightedRoundRobin wrr = new WeightedRoundRobin(names, probs);
		
		for(int i = 0;i<repeats;i++){
			counts[Integer.parseInt(wrr.getNext())]++;
		}
		
		
		//System.out.println(Arrays.toString(counts));
		float prev = 0;
		for (int i = 0; i < names.length; i++) {
			float now = wrr.getBorders()[i] - prev;
			prev = wrr.getBorders()[i];
			assertEquals("probilistic case,...",now,1.0f*counts[i]/repeats,0.01);
		}
		
	}

}
