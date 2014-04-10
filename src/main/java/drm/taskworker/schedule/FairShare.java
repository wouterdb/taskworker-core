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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import drm.taskworker.Job;
import drm.taskworker.Service;
import drm.taskworker.config.Config;
import drm.taskworker.tasks.JobStateListener;

/**
 * @author wouter
 * 
 * 
 *         FIXME: locking!
 *         
 *         
 */
@SuppressWarnings("rawtypes")
public class FairShare implements IScheduler, JobStateListener {

	private List<String> workers;

	@SuppressWarnings("unchecked")
	@Override
	public void enable(Map config) {
		

		this.workers = getWorkers();
		Service.get().addWorkflowStateListener(this);
	}

	@Override
	public synchronized void jobStarted(Job job) {
		List<String> jobs = recoverJobs();

		jobs.add(job.getJobId().toString());
		rebuild(jobs);
	}

	private static List<String> recoverJobs() {
		List<String> jobs = new LinkedList<>();
		// attempt to recover old data
		WeightedRoundRobin old = null;

		for (String w : getWorkers()) {
			old = Service.get().getPriorities(w);
			if (old != null) {
				break;
			}
		}

		if (old != null) {
			jobs.addAll(Arrays.asList(old.getNames()));
		}
		
		return jobs;
	}

	
	
	public static void removejob(Job job) {
		List<String> jobs = recoverJobs();
		jobs.remove(job.getJobId().toString());
		Service.get().removeJobPriority(job, getWorkers());
		rebuild(jobs);
	}

	private static void rebuild(List<String> jobs) {
		float[] weights = new float[jobs.size()];
		Arrays.fill(weights, 1.0f);

		WeightedRoundRobin wrr = new WeightedRoundRobin(
				jobs.toArray(new String[jobs.size()]), weights);

		for (String worker : getWorkers()) {
			Service.get().setPriorities(worker, wrr);
		}
	}

	private static List<String> getWorkers() {
		List<String> workers = (List<String>) Config.cfg().getScheduler().getArguments().get("workers");
		if (workers == null) {
			workers = new ArrayList<>(Config.getConfig().getWorkers().keySet());
		}
		return workers;
	}

	@Override
	public void jobFinished(Job job) {
		removejob(job);		
	}

}
