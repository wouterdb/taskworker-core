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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import drm.taskworker.config.Config;

/**
 * This class starts pull workers for processing tasks from pull queues
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class WorkerRegistration {
	private List<Worker> background_threads = null;
	private Config config = null;
	protected static final Logger logger = Logger.getLogger(Worker.class
			.getCanonicalName());

	public WorkerRegistration(File file) {
		background_threads = new ArrayList<Worker>();
		try {
			InputStream input = new FileInputStream(file);
			config = Config.loadConfig(input);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void addWorker(Worker worker) {
		Thread thread = new Thread(worker);
		thread.setDaemon(true);
		this.background_threads.add(worker);
		thread.start();
	}

	public void start() {
		// first ensure that the keyspace exists
		Entities.cs();
		
		if (config.getScheduler() != null) {
			logger.info("Starting scheduler");
			//scheduler means master server
			config.getScheduler().create();

			// start a thread to manage the queue service
			Thread thread = new Thread(new Runnable() {
						@Override
						public void run() {
							while (true) {
								Service.get().startJobs();
								try {
									Thread.sleep(2000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					});
			
			thread.setDaemon(true);
			thread.start();
		}

		for (drm.taskworker.config.WorkerConfig worker : config.getWorkers().values()) {
			logger.info("Starting " + worker.getWorkerName() + " thread");
			for (int i = 0; i < worker.getThreads(); i++) {
				Worker w = worker.getWorkerInstance();
				this.addWorker(w);
			}
		}

	}

	/**
	 * Stop all worker threads when the servlet is destroyed.
	 */
	public void stop() {
		for (Worker thread : this.background_threads) {
			thread.stopWorking();
		}
	}
}
