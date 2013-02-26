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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.UUID;

import com.google.appengine.api.taskqueue.TaskOptions;

/**
 * A baseclass for all tasks.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class AbstractTask implements Serializable {
	private String worker = null;
	private UUID workflowId = null;
	
	/**
	 * Create a task for a worker
	 * 
	 * @param worker The name of the worker
	 */
	public AbstractTask(String worker) {
		this.worker = worker;
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
	 * Get the unique workflow id.
	 * 
	 * @return the workflowId
	 */
	public UUID getWorkflowId() {
		return workflowId;
	}

	/**
	 * Set the workflow id of this task. This workflow cannot be changed, if the
	 * id is already set and this method is called again, an
	 * IllegalArgumentException is thrown.
	 */
	public final void setWorkFlowId(UUID uuid) {
		if (this.workflowId == null) {
			this.workflowId = uuid;
		} else {
			throw new IllegalArgumentException();
		}
	}
}
