package drm.taskworker.monitoring;

import dnet.minimetrics.MiniMetrics;
import dnet.minimetrics.Timer;

public class Metrics {

	public static Timer timer(String name) {
		return MiniMetrics.get().getTimer(name);
	}

	

}
