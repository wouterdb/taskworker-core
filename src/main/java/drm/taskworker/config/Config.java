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

package drm.taskworker.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

/**
 * The configuration of the workers
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Config {
	private static Config config = null;
	private Map<String, WorkerConfig> workers = null;
	private Map<String, WorkflowConfig> workflows = null;
	private SchedulerConfig scheduler = null;
	private static Logger logger = Logger.getLogger(Config.class.getCanonicalName());
	
	/**
	 * The default constructor for the configuration.
	 */
	private Config() {
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Config loadConfig() {
		String path = System.getProperty("dreamaas.configfile", "config.yaml");
		logger.info("Loading configuration file " + path);
		
		File file = new File(path);
		if (!file.canRead()) {
			System.err.println(path + " is not readable.");
			System.exit(1);
		}
		
		Yaml yaml = new Yaml();
		Map data;
		try {
			data = (Map) yaml.load(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("Unable to load config yaml file.");
		}

		Config cfg = new Config();
		cfg.setWorkers(WorkerConfig.parseWorkers((List) data.get("workers")));
		cfg.setWorkflows(WorkflowConfig.parseWorkflows((Map) data.get("workflows")));
		cfg.setScheduler(SchedulerConfig.parseScheduler((Map) data.get("scheduler")));
		
		return cfg;
	}
	
	public static Config getConfig() {
		if (config == null) {
			config = loadConfig();
		}

		return config;
	}
	
	/**
	 * @return the workflows
	 */
	public Map<String, WorkflowConfig> getWorkflows() {
		return workflows;
	}

	/**
	 * @param workflows the workflows to set
	 */
	public void setWorkflows(Map<String, WorkflowConfig> workflows) {
		this.workflows = workflows;
	}

	/**
	 * @return the workers
	 */
	public Map<String, WorkerConfig> getWorkers() {
		return workers;
	}

	/**
	 * @param workers the workers to set
	 */
	public void setWorkers(Map<String, WorkerConfig> workers) {
		this.workers = workers;
	}

	

	public void setScheduler(SchedulerConfig sched) {
		this.scheduler  = sched;
		
	}
	
	public SchedulerConfig getScheduler() {
		return scheduler;
	}

	
	/**
	 * Get the workflow with the given name.
	 * @param name
	 * @return
	 */
	public WorkflowConfig getWorkflow(String name) {
		if (!this.workflows.containsKey(name)) {
			throw new IllegalArgumentException("There is no workflow with name " + name);
		}
		
		return this.workflows.get(name);
	}
}
