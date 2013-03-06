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

	public static final int repeats = 10000;
	
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
