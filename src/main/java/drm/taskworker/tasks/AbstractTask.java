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

package drm.taskworker.tasks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.google.appengine.api.taskqueue.TaskOptions;

import drm.taskworker.Workflow;

/**
 * A baseclass for all tasks.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class AbstractTask implements Serializable {
	private String worker = null;
	private String symbolWorker = null;
	private Workflow workflow = null;
	private AbstractTask parent = null;
	
	/**
	 * Create a task for a worker
	 * 
	 * @param workflow The workflow this task belongs to
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	public AbstractTask(Workflow workflow, AbstractTask parentTask, String worker) {
		this.workflow = workflow;
		this.parent = parentTask;
		this.setSymbolWorker(worker);
		
		// lookup the next worker
		if (this.parent != null) {
			this.worker = workflow.resolveStep(this.parent.getWorker(), worker);
		} else {
			this.worker = worker;
		}
	}
	
	/**
	 * Returns the string id of this type of task
	 * 
	 * @return A string that can be used to determine the type of task.
	 */
	public abstract String getTaskType();
	
	/**
	 * Convert this task to a taskoption object.
	 * 
	 * @return
	 */
	public TaskOptions toTaskOption() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this);
		
	    TaskOptions to = TaskOptions.Builder.withMethod(TaskOptions.Method.PULL);
		to.payload(bos.toByteArray());
		
		oos.close();
		bos.close();
		
		to.tag(this.getWorker());
		
		return to;
	}
	
	/**
	 * Get the name of the work to send the task to.
	 * 
	 * @return A string that determines the name of the worker.
	 */
	public String getWorker() {
		return this.worker;
	}
	
	/**
	 * Get the workflow this task belongs to
	 * @return
	 */
	public Workflow getWorkflow() {
		return this.workflow;
	}

	/**
	 * @return the symbolWorker
	 */
	public String getSymbolWorker() {
		return symbolWorker;
	}

	/**
	 * @param symbolWorker the symbolWorker to set
	 */
	private void setSymbolWorker(String symbolWorker) {
		this.symbolWorker = symbolWorker;
	}
}
