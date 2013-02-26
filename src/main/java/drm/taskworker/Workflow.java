/*
    Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    Administrative Contact: dnet-project-office@cs.kuleuven.be
    Technical Contact: bart.vanbrabant@cs.kuleuven.be
*/

package drm.taskworker;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;

import drm.taskworker.config.Config;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.StartTask;
import drm.taskworker.tasks.Task;

/**
 * This class manages a workflow
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Workflow implements Serializable {
	private static Logger logger = Logger.getLogger(Worker.class.getCanonicalName());
	
	private String name = null;
	private drm.taskworker.config.WorkflowConfig workflowConfig = null;
	private UUID workflowId = null;
	
	public Workflow(String name) {
		this.workflowId = UUID.randomUUID();
		this.setName(name);
		this.loadConfig();
	}

	/**
	 * Load the configuration of this workflow
	 */
	private void loadConfig() {
		Config cfg = Config.getConfig();
		this.workflowConfig  = cfg.getWorkflow(this.name);
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Resolve a symbolic next step for the given current step. If the next
	 * symbol is not defined, the next symbol parameter itself will be returned. 
	 * 
	 * @param stepName The step to lookup the next step for
	 * @param nextSymbol The next symbol that needs to be resolved.
	 * @return The actual next step.
	 */
	public String resolveStep(String stepName, String nextSymbol) {
		return this.workflowConfig.getNextStep(stepName, nextSymbol);
	}
	
	/**
	 * Start a new workflow
	 * 
	 * @param task
	 * @return
	 */
	public void startNewWorkflow(StartTask task) throws IOException {
		Queue q = QueueFactory.getQueue("pull-queue");
		q.add(task.toTaskOption());
			
		// send end of workflow as the last task
		EndTask endTask = new EndTask(task, task.getWorker());
		q.add(endTask.toTaskOption());
		
		logger.info("Started workflow. Added task for " + task.getWorker());
	}
	
	/**
	 * Get a new task in this workflow
	 * 
	 * @param worker The work on which this task should be allocated.
	 * @return A new task that can be added to the queue 
	 */
	public Task newTask(Task parent, String worker) {
		return new Task(parent, worker);
	}
	
	/**
	 * The string representation of this workflow.
	 */
	@Override
	public String toString() {
		return "workflow-" + this.workflowId.toString();
	}
	
	/**
	 * Get the unique id of this workflow.
	 * @return
	 */
	public UUID getWorkflowId() {
		return this.workflowId;
	}

	/**
	 * Create a new task that starts the workflow
	 */
	public StartTask newStartTask() {
		return new StartTask(this, this.workflowConfig.getWorkflowStart());
	}
}
