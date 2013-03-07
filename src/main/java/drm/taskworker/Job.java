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

import java.util.Date;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.NumericField;

import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.WorkflowInstance;

/**
 * A job is an instance of a workflow and a first task. A job is either
 * started immediately or at a future point in time.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Job {

	private WorkflowInstance workflow;
	private Task startTask;
	
	private long startAt = 0;
	private long finishAt = 0;
	
	/**
	 * Create a new job that is started immediately
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 */
	public Job(WorkflowInstance workflow, Task startTask) {
		this(workflow, startTask, new Date(), null);
	}

	/**
	 * Create a new job that is started immediately
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 * @param startAt Start the task at this moment
	 * @param finishAt Finish the task by that time
	 */
	public Job(WorkflowInstance workflow, Task startTask, Date startAt,
			Date finishAt) {
		super();
		this.workflow = workflow;
		this.startTask = startTask;
		
		setStartAt(startAt.getTime());
		
		if (finishAt != null) {
			setFinishAt(finishAt.getTime());
		}
	}
	

	/**
	 * Create a new job that is started at the given date but does not have
	 * a deadline.
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 * @param startAt Start the task at this moment
	 */
	public Job(WorkflowInstance workflow, Task startTask, Date startAt) {
		this(workflow, startTask, startAt, null);
	}
	
	/**
	 * Get the workflow instance
	 */
	public WorkflowInstance getWorkflow() {
		return workflow;
	}

	/**
	 * The first task in the workflow
	 * 
	 * @return
	 */
	public Task getStartTask() {
		return startTask;
	}

	/**
	 * Get the start time of this job
	 * 
	 * @return
	 */
	@Field
	@NumericField
	public long getStartAt() {
		return this.startAt;
	}
	
	/**
	 * Set the start time of this job in a timestamp in milliseconds.
	 * 
	 * @param startAt
	 */
	public void setStartAt(long startAt) {
		this.startAt = startAt;
	}

	/**
	 * Finish the workflow by this point in time. This value can be 0 to 
	 * indicate that this job does not have a deadline.
	 * 
	 * @return
	 */
	@Field
	@NumericField
	public long getFinishAt() {
		return finishAt;
	}

	/**
	 * Set the time when the job has to finish.
	 * 
	 * @param time
	 */
	public void setFinishAt(long time) {
		if (time > 0 && time < this.startAt) {
			throw new IllegalArgumentException("The finish time should be set after the start time of the job.");
		}
		this.finishAt = time;
	}
}
