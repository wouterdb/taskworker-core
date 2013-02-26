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

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * The configuration of the workers
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Config implements Serializable {
	private static Config config = null;
	private Map<String, WorkerConfig> workers = null;
	private Map<String, WorkflowConfig> workflows = null;
	
	/**
	 * The default constructor for the configuration.
	 */
	private Config() {
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Config loadConfig(InputStream yamlConfigFile) {
		if (config == null) {
			Yaml yaml = new Yaml();
			Map data = (Map) yaml.load(yamlConfigFile);
	
			Config cfg = new Config();
			cfg.setWorkers(WorkerConfig.parseWorkers((List) data.get("workers")));
			cfg.setWorkflows(WorkflowConfig.parseWorkflows((Map) data.get("workflows")));
			
			config = cfg;
		}

		return config;
	}
	
	public static Config getConfig() {
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
