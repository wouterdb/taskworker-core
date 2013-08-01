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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The configuration of a worker.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class WorkerConfig {
	private String workerName = null;
	private String workerClass = null;
	private int threads = 1;
	
	public WorkerConfig(String workerName, String workerClass) {
		this.workerClass = workerClass;
		this.workerName = workerName;
	}
	
	/**
	 * @return the workerName
	 */
	public String getWorkerName() {
		return workerName;
	}
	/**
	 * @param workerName the workerName to set
	 */
	public void setWorkerName(String workerName) {
		this.workerName = workerName;
	}
	/**
	 * @return the workerClass
	 */
	public String getWorkerClass() {
		return workerClass;
	}
	/**
	 * @param workerClass the workerClass to set
	 */
	public void setWorkerClass(String workerClass) {
		this.workerClass = workerClass;
	}
	
	/**
	 * Get an instance of the given worker
	 * 
	 * @return An instance of the worker class
	 */
	@SuppressWarnings("unchecked")
	public drm.taskworker.Worker getWorkerInstance() {
		try {
			Class<drm.taskworker.Worker> workerCls = (Class<drm.taskworker.Worker>)Class.forName(this.getWorkerClass());
			Constructor<drm.taskworker.Worker> workerCtor = workerCls.getConstructor(String.class);
			return workerCtor.newInstance(this.getWorkerName());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("Class " + this.workerClass + " should have a constructor that accepts the name of the worker.");
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Create a list of worker
	 */
	@SuppressWarnings("unchecked")
	public static Map<String,WorkerConfig> parseWorkers(@SuppressWarnings("rawtypes") List<Map> workers) {
		Map<String,WorkerConfig> results = new HashMap<String, WorkerConfig>();
		for (Map<String,Object> map : workers) {
			if (!map.containsKey("name") || ! map.containsKey("class")) {
				throw new IllegalArgumentException("Each worker should have name and class attributes set in the config file.");
			}
			
			WorkerConfig obj = new WorkerConfig((String)map.get("name"), (String)map.get("class"));
			obj.setThreads(Integer.valueOf((Integer)map.get("threads")));
			
			results.put((String)map.get("name"), obj);
		}
		
		return results;
	}
	
	/**
	 * Set the number of threads that should be started
	 */
	public void setThreads(int threads) {
		this.threads  = threads;
	}
	
	/**
	 * Get the number of threads
	 */
	public int getThreads() {
		return this.threads;
	}
}
