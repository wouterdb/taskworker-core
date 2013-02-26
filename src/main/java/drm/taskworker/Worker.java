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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskHandle;

/**
 * A work class that fetches work from a pull queue
 * 
 * @author bart
 * 
 */
public abstract class Worker implements Runnable {
	protected static final Logger logger = Logger.getLogger(Worker.class
			.getCanonicalName());

	private String name = null;
	private boolean working = true;

	/**
	 * Create a new work with a name
	 * 
	 * @param name
	 *            The name of the work. This names should be unique
	 */
	public Worker(String name) {
		this.name = name;
	}

	/**
	 * Do the work for the given task.
	 */
	public abstract TaskResult work(Task task);

	/**
	 * Signal the end of the workflow.
	 */
	public abstract TaskResult work(EndTask task);

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
		while (this.working) {
			try {
				Queue q = QueueFactory.getQueue("pull-queue");
				List<TaskHandle> tasks = q.leaseTasksByTag(10,
						TimeUnit.SECONDS, 1, this.name);
				
				// this check is a safeguard against bad queue behavior
				if (tasks.size() > 1) {
					throw new IllegalStateException("Queue returns more results than requested");
				}
				
				// do work!
				if (!tasks.isEmpty()) {
					TaskHandle handle = tasks.get(0);

					ObjectInputStream ois = new ObjectInputStream(
							new ByteArrayInputStream(handle.getPayload()));
					AbstractTask task = (AbstractTask) ois.readObject();
					ois.close();

					logger.info("Fetched task " + task.toString() + " for "
							+ this.name + " with id " + handle.getName());

					TaskResult result = null;
					if (task.getTaskType() == "work" || task.getTaskType() == "start") {
						result = this.work((Task) task);
					} else if (task.getTaskType() == "end") {
						result = this.work((EndTask) task);
					} else {
						logger.warning("Task type " + task.getTaskType()
								+ " not known.");
						continue;
					}

					if (result == null) {
						logger.warning("Worker returns null. Ouch ...");
						continue;
					}

					if (result.getResult() == TaskResult.Result.SUCCESS) {
						for (AbstractTask newTask : result.getNextTasks()) {
							newTask.setWorkFlowId(task.getWorkflowId());
							TaskHandle newTH = q.add(newTask.toTaskOption());
							
							logger.info("New task for " + newTask.getWorker()
									+ " on worker " + this.name
									+ " added with id " + newTH.getName());
						}
					} else {
						logger.info("Task " + task.toString() + " failed with "
								+ result.getResult().toString());
						
						if (result.getResult() == TaskResult.Result.EXCEPTION) {
							result.getException().printStackTrace();
						}
					}
					q.deleteTask(handle);
				}

				Thread.sleep(100);
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
