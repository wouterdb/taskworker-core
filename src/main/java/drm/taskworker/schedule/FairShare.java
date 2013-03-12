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

import org.apache.cassandra.scheduler.RoundRobinScheduler;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.Service;
import drm.taskworker.config.Config;
import drm.taskworker.tasks.WorkFlowStateListener;
import drm.taskworker.tasks.WorkflowInstance;

/**
 * @author wouter
 * 
 * 
 *         //fixme: preload list of open worklows after restart
 */
public class FairShare implements IScheduler, WorkFlowStateListener {

	private List<String> workers;
	private List<String> workflows = new LinkedList<>();

	@Override
	public void enable(Map config) {
		List<String> workers = (List<String>) config.get("workers");
		if (workers == null) {
			workers = new ArrayList<>(Config.getConfig().getWorkers().keySet());
		}

		this.workers = workers;
		Service.get().addWorkflowStateListener(this);
		
		
		//attempt to recover old data
		WeightedRoundRobin old = null;
		
		for(String w:workers){
			old = Service.get().getPriorities(w);
			if(old!=null)
				break;
		}
			
		if(old!=null){
			workflows.addAll(Arrays.asList(old.getNames()));
		}
		rebuild();
	}

	@Override
	public synchronized void workflowStarted(WorkflowInstance wf) {
		workflows.add(wf.getWorkflowId().toString());
		System.out.println("added: " +wf.getWorkflowId().toString() );
		rebuild();
	}

	@Override
	public synchronized void workflowFinished(WorkflowInstance wf) {
		workflows.remove(wf.getWorkflowId().toString());
		
		System.out.println("removed: " +wf.getWorkflowId().toString() );
		rebuild();

	}

	private void rebuild() {
		float[] weights = new float[workflows.size()];
		Arrays.fill(weights, 1.0f);
		
		WeightedRoundRobin wrr = new WeightedRoundRobin(workflows.toArray(new String[workflows.size()]), weights);
		for(String worker:workers)
			Service.get().setPriorities(worker, wrr);
	}

}
