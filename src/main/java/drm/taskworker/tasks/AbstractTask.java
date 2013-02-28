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
import java.util.UUID;

import com.google.appengine.api.taskqueue.TaskOptions;
import com.googlecode.objectify.Ref;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Parent;

import drm.taskworker.Workflow;

import static com.googlecode.objectify.ObjectifyService.ofy;


/**
 * A baseclass for all tasks.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
@Entity
public abstract class AbstractTask implements Serializable {
	@Id private String taskId = null;
	
	private String worker = null;
	private String symbolWorker = null;
	
	/*
	 * The link to the workflowRef is transient because it is not possible to
	 * serialize the ref when we place a task on the queue. To be able to 
	 * restore this ref, we need to store the workflow id as well so we can
	 * rebuild the ref.
	 */
	private String workflowId = null;
	@Parent transient private Ref<Workflow> workflowRef = null;
	
	/*
	 * Same comment here as for workflow 
	 */
	private String parentId = null;
	transient private Ref<AbstractTask> parentRef = null;
	
	/**
	 * Create a task for a worker
	 * 
	 * @param workflow The workflow this task belongs to
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	public AbstractTask(Workflow workflow, AbstractTask parentTask, String worker) {
		this();
		
		this.workflowRef = Ref.create(workflow);
		this.workflowId = workflow.getWorkflowId();
		
		this.setSymbolWorker(worker);
		
		// lookup the next worker
		if (parentTask != null) {
			this.parentRef = Ref.create(parentTask);
			this.parentId = parentTask.getId();
			this.worker = workflow.resolveStep(this.getParentTask().getWorker(), worker);
		} else {
			this.worker = worker;
		}
	}
	
	public AbstractTask() {
		this.taskId = UUID.randomUUID().toString();
	}
	
	/**
	 * Get the id of this task.
	 */
	public String getId() {
		return this.taskId;
	}
	
	/**
	 * Get the parent of this task.
	 */
	public AbstractTask getParentTask() {
		if (this.parentId == null) {
			// this is a root node
			return null;
		}
		
		if (this.parentRef == null) {
			this.parentRef = ofy().load().type(AbstractTask.class).id(this.parentId);
		}
		return this.parentRef.safeGet();
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
	 * Get the workflow this task belongs to.
	 */
	public Workflow getWorkflow() {
		if (this.workflowRef == null) {
			this.workflowRef = ofy().load().type(Workflow.class).id(this.workflowId);
		}
		Workflow wf = this.workflowRef.safeGet();
		return wf;
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
