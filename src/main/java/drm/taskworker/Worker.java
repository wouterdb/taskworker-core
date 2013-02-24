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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;

/**
 * A work class that fetches work from a pull queue
 * 
 * @author bart
 * 
 */
public abstract class Worker implements Runnable {
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
	 * 
	 */
	public abstract TaskResult work(Task task);
	
	public void stopWorking() {
		this.working = false;
	}

	@Override
	public void run() {
		while (this.working) {
			try {
				Queue q = QueueFactory.getQueue("pull-queue");
				// get one task
				List<TaskHandle> tasks = q.leaseTasksByTag(1000, TimeUnit.SECONDS, 1, this.name);

				if (!tasks.isEmpty()) {
				    System.out.println("Fetched task for " + this.name);
					TaskHandle handle = tasks.get(0);

					ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(handle.getPayload()));
					Task task = (Task)ois.readObject();
					ois.close();
					
					TaskResult result = this.work(task);
					System.out.println("Work done");
					
					if (result.getResult() == TaskResult.Result.SUCCESS) {
						for (Task newTask : result.getNextTasks()) {
							System.out.println("New task for " + newTask.getWorker() + " on worker " + this.name);
							ByteArrayOutputStream bos = new ByteArrayOutputStream();
							ObjectOutputStream oos = new ObjectOutputStream(bos);
							oos.writeObject(newTask);
							
						    TaskOptions to = TaskOptions.Builder.withMethod(TaskOptions.Method.PULL);
							to.payload(bos.toByteArray());
							
							oos.close();
							bos.close();
							
							to.tag(newTask.getWorker());
						    
							System.out.println("Added new task for " + newTask.getWorker());
						    q.add(to);
						}
					}
					
					System.out.println("Deleted task from queue");
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
