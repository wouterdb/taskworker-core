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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.TaskHandle;

import com.yammer.metrics.Counter;
import com.yammer.metrics.Timer;
import com.yammer.metrics.Timer.Context;

import drm.taskworker.monitoring.Metrics;
import drm.taskworker.queue.Queue;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * A work class that fetches work from a pull queue
 * 
 * TODO: add workflow abort TODO: mark jobs in the database as started and
 * finished
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class Worker implements Runnable {
	protected static final Logger logger = Logger.getLogger(Worker.class
			.getCanonicalName());

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
	 * Get the name of the next worker
	 */
	public String getNextWorker() {
		return this.nextWorker;
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
		result.addNextTask(new EndTask(task, this.getNextWorker()));
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
		int sleepTime = 10;

		Timer fetching = Metrics.getNormalRegistry().timer("worker."+getName() + ".fetch");
		Timer sleeping = Metrics.getNormalRegistry().timer("worker."+getName() + ".sleep");
		
		while (this.working) {
			try {
				
				Context tc = fetching.time();
				TaskHandle handle = svc.getTask(this.name);
				tc.stop();
				
				if (handle != null) {
					ObjectInputStream ois = new ObjectInputStream(
							new ByteArrayInputStream(handle.getPayload()));
					AbstractTask task = (AbstractTask) ois.readObject();
					ois.close();

					logger.info("Fetched task " + task.toString() + " for "
							+ this.name + " with id " + handle.getName());

					// execute the task
					TaskResult result = null;
					task.setStartedAt();
					if (task.getTaskType().equals("work")) {
						result = this.work((Task) task);

					} else if (task.getTaskType().equals("end")) {
						/*
						 * This is an end task. If we get this task, we need to
						 * ensure that all other tasks of this workflow have
						 * been finished.
						 * 
						 * TODO: implement this
						 */
						result = this.work((EndTask) task);
					} else {
						logger.warning("Task type " + task.getTaskType()
								+ " not known.");
						continue;
					}
					task.setFinishedAt();
					task.saveTiming();

					if (result == null) {
						logger.warning("Worker returns null. Ouch ...");
						continue;
					}

					if (result.getResult() == TaskResult.Result.SUCCESS) {
						if (this.getName().equals(
								task.getWorkflow().getWorkflowConfig()
										.getWorkflowEnd())) {
							// this is the end of the workflow
							// if end-of-batch, signal end-of-job
							if (task.getTaskType().equals("end")) {
								svc.workflowFinished(task,
										result.getNextTasks());
							}
							
						} else {
							for (AbstractTask newTask : result.getNextTasks()) {
								svc.queueTask(newTask);
								logger.info("New task for "
										+ newTask.getWorker() + " on worker "
										+ this.name);
							}
						}
					} else {
						logger.info("Task " + task.toString() + " failed with "
								+ result.getResult().toString() + " on "
								+ this.name);

						if (result.getResult() == TaskResult.Result.EXCEPTION) {
							result.getException().printStackTrace();
						}
					}
					svc.deleteTask(handle);

					sleepTime = sleepTime - 10;
					if (sleepTime < 0) {
						sleepTime = 0;
					}
				} else {
					sleepTime += 10;
					if (sleepTime > 200) {
						sleepTime = 200;
					}
					Context tcs = sleeping.time();
					Thread.sleep(sleepTime);
					tcs.stop();
				}
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}
