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

import java.util.Arrays;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.ArrayUtils;

import com.codahale.metrics.Timer.Context;

import drm.taskworker.monitoring.Metrics;
import drm.taskworker.schedule.WeightedRoundRobin;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;
import drm.taskworker.tasks.TaskResult.Result;

/**
 * A work class that fetches work from a pull queue
 * 
 * TODO: add workflow abort 
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class Worker implements Runnable {
	protected static final Logger logger = 
							Logger.getLogger(Worker.class.getCanonicalName());

	private String name = null;

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	private boolean working = true;
	private String nextWorker = "next";

	/**
	 * Create a new work with a name
	 * 
	 * @param name
	 *            The name of the work. This names should be unique
	 */
	public Worker(String name) {
		this.name = name;
		logger.info("Worker started with name " + this.name);
	}

	/**
	 * Get the name of the next worker if the workflow id is known
	 */
	public String getNextWorker(UUID workflowId) {
		return Service.get().getNextWorker(workflowId, this.getName(), this.nextWorker);
	}

	/**
	 * Set the name of the next worker
	 */
	protected void setNextWorker(String name) {
		this.nextWorker = name;
	}

	/**
	 * Do the work for the given task.
	 */
	public abstract TaskResult work(Task task);

	/**
	 * Handle the end of workflow token by sending it to the next hop.
	 */
	public TaskResult work(EndTask task) {
		TaskResult result = new TaskResult();
		result.addNextTask(new EndTask(task, this.getNextWorker(task.getJobId())));
		return result.setResult(TaskResult.Result.SUCCESS);
	}

	/**
	 * Stop working so the thread ends clean
	 */
	public void stopWorking() {
		this.working = false;
	}
	
	/**
	 * The main loop that handles the tasks.
	 */
	@Override
	public void run() {
		logger.info("Started worker " + this.toString());

		Service svc = Service.get();
		int sleepTime = 200;

		while (this.working) {
			try {
				Context tcLease = Metrics.timer("worker.lease").time();
				Context tcNoLease = Metrics.timer("worker.nolease").time();
				AbstractTask task = svc.getTask(this.name);
				
				if (task != null) {
					tcLease.stop();
					Context tc = Metrics.timer("worker.work." + this.name).time();

					trace("FETCHED",task);

					// execute the task
					TaskResult result = null;
					task.setStartedAt();
					try {
						if (task.getTaskType() == 0) {
							result = this.work((Task) task);

						} else if (task.getTaskType() == 1) {
							result = this.work((EndTask) task);
						} else {
							logger.warning("Task type " + task.getTaskType() + " not known.");
							continue;
						}

					} catch (Exception e) {
						result = new TaskResult();
						
						result.setException(e);
						result.setResult(Result.EXCEPTION);
						result.fail();
					}
					task.setFinishedAt();
					task.saveTiming();

					if (result == null) {
						result = new TaskResult();
						result.setResult(Result.ERROR);
						result.fail();
						logger.warning("Worker returns null. Ouch ...");
					}

					// process the result
					if (result.getResult() == TaskResult.Result.SUCCESS) {
						trace("DONE", task);
						if (svc.isJobEnd(task.getJobId(), this.getName())) {
							// this is the end of the workflow
							// if end-of-batch, signal end-of-job
							if (task.getTaskType() == 1) {
								svc.workflowFinished(task, result.getNextTasks());
							}
						} else {
							for (AbstractTask newTask : result.getNextTasks()) {
								svc.queueTask(newTask);
								trace("NEW", newTask);
							}
							if (task.getTaskType() == 1) {
								removeFromSchedule(task);
							}
						}
						svc.deleteTask(task);
						
					} else {
						trace("FAILED", task);
						logger.warning(String.format("[%s] failed %s: %s", this.name, task.toString(), result.getResult().toString())); 
						if (result.getResult() == TaskResult.Result.EXCEPTION) {
							result.getException().printStackTrace();
						}
						
						if (result.isFatal()) {
							// if this task is fatal, kill the current workflow
							svc.killJob(task.getJobId());
						}
					}
					
					sleepTime = sleepTime - 10;
					if (sleepTime < 0) {
						sleepTime = 0;
					}
					
					tc.stop();
				} else {
					tcNoLease.stop();
					sleepTime += 10;
					if (sleepTime > 200) {
						sleepTime = 200;
					}
				}

				Thread.sleep(sleepTime);
			} catch (Exception e) {
				logger.log(Level.SEVERE, getName() + " failed", e);
			}
		}
	}

	private void trace(String cmd, AbstractTask task) {
		logger.info(String.format("[%s] %s %s", this.name, cmd, task.toString()));
	}

	/**
	 * This method is called when an endtask has been processed. It removes 
	 * the workflow for the current worker from the priorities table.
	 * 
	 * @param task
	 */
	private void removeFromSchedule(AbstractTask task) {
		Service svc = Service.get();
		WeightedRoundRobin wrr = svc.getPriorities(this.name);
		int i = 0;
		UUID workflowId = task.getJobId();
		String[] names = wrr.getNames();
		for (; i < names.length; i++) {
			if (names[i].equals(workflowId.toString()))
				break;
		}
		if (i == names.length)
			return;
		int size = names.length;

		float[] weights = new float[size - 1];
		Arrays.fill(weights, 1.0f);
		wrr = new WeightedRoundRobin((String[]) ArrayUtils.remove(
				wrr.getNames(), i), weights);
		
		svc.setPriorities(this.name, wrr);
	}
}
