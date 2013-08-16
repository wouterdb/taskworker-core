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

import static drm.taskworker.config.Config.cfg;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import drm.taskworker.rest.RestServer;

/**
 * The main application
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class App {
	private static Logger logger = Logger.getLogger(App.class.getCanonicalName());
		
	/**
	 * @param args
	 * @throws IOException 
	 * @throws SecurityException 
	 */
	public static void main(String[] args) throws SecurityException, IOException {
		InputStream logging_config = App.class.getClassLoader().getResourceAsStream("logging.properties");
		if (logging_config != null) {
			LogManager.getLogManager().readConfiguration(logging_config);
		}
		
		// ensure that the connection with cassandra is functioning
		Entities.cs();
		
		// start components
		if (cfg().getProperty("dreamaas.workers", true)) {
			WorkerRegistration wr = new WorkerRegistration();
			wr.start();
		}
		
		if (cfg().getProperty("dreamaas.scheduler", false)) {
			logger.info("Starting scheduler");
			
			//scheduler means master server
			cfg().getScheduler().create();

			// start a thread to manage the queue service
			Thread thread = new Thread(new Runnable() {
						@Override
						public void run() {
							while (true) {
								Service.get().startJobs();
								try {
									Thread.sleep(cfg().getProperty("dreamaas.scheduler.interval", 2) * 1000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						}
					});
			
			thread.setDaemon(true);
			thread.start();
		}
		
		RestServer server = null;
		if (cfg().getProperty("dreamaas.rest", false)) {
			server = new RestServer();
			server.start();
		}

		// wait forever
		while(true) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

}
