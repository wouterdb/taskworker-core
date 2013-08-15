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
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

import drm.taskworker.config.Config;
import drm.taskworker.rest.RestServer;

/**
 * The main application
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class App {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SecurityException 
	 */
	public static void main(String[] args) throws SecurityException, IOException {
		if (args.length != 1) {
			System.err.println("A path to the configuration file is required.");
			System.exit(1);
		}
		
		InputStream logging_config = App.class.getClassLoader().getResourceAsStream("logging.properties");
		if (logging_config != null) {
			LogManager.getLogManager().readConfiguration(logging_config);
		}
		
		String path = args[0];
				
		File file = new File(path);
		if (!file.canRead()) {
			System.err.println(path + " is not readable.");
			System.exit(1);
		}
		
		InputStream input = new FileInputStream(file);
		Config.loadConfig(input);
		
		WorkerRegistration wr = new WorkerRegistration();
		wr.start();
		
		new RestServer().start();

		// wait forever
		while(true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
