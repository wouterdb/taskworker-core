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

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.google.appengine.api.ThreadManager;

import drm.taskworker.workers.BlobWorker;
import drm.taskworker.workers.CSVInvoiceWorker;
import drm.taskworker.workers.TemplateWorker;
import drm.taskworker.workers.XslFoRenderWorker;
import drm.taskworker.workers.ZipWorker;

/**
 * This class starts pull workers for processing tasks from pull queues
 * 
 * @author Bart Vanbrabant
 */
public class PullWorkerRegistration implements ServletContextListener {
	private List<Worker> background_threads = null;
	
	public PullWorkerRegistration() {
		background_threads = new ArrayList<Worker>();
	}
	
	private void addWorker(Worker worker) {
		Thread thread = ThreadManager.createBackgroundThread(worker);
		this.background_threads.add(worker);
		thread.start();
	}

	public void contextInitialized(ServletContextEvent event) {
		this.addWorker(new BlobWorker());
		this.addWorker(new XslFoRenderWorker());
		this.addWorker(new CSVInvoiceWorker());
		this.addWorker(new TemplateWorker());
		this.addWorker(new ZipWorker());
	}

	/**
	 * Stop all worker threads when the servlet is destroyed.
	 */
	public void contextDestroyed(ServletContextEvent event) {
		for (Worker thread : this.background_threads) {
			thread.stopWorking();
		}
	}
}
