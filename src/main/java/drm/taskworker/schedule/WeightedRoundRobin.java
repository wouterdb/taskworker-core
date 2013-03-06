package drm.taskworker.schedule;

import java.util.Arrays;

public class WeightedRoundRobin {

	private final float[] borders;
	private final String[] names;

	public WeightedRoundRobin(String[] names, float[] weights) {
		if (names.length != weights.length)
			throw new IllegalArgumentException("lengths do not match");
		this.names = names;
		this.borders = new float[names.length];
		float sum = 0;
		for (int i = 0; i < weights.length; i++) {
			sum += weights[i];
		}
		float runningsum = 0;
		for (int i = 0; i < weights.length; i++) {
			runningsum += weights[i] / sum;
			this.borders[i] = runningsum;
		}
	}

	public float[] getBorders() {
		return borders;
	}

	public String[] getNames() {
		return names;
	}

	public String getNext(){
		int index = Arrays.binarySearch(borders, (float)Math.random());
		if(index<0){
			return names[-index-1];
		}
		return names[index];
	}
}
