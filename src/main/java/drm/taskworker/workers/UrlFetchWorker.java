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

package drm.taskworker.workers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import drm.taskworker.Worker;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * A worker that fetches webpages and stores them.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class UrlFetchWorker extends Worker {
	public UrlFetchWorker(String name) {
		super(name);
	}

	/**
	 * A service that fetches urls
	 * 
	 * @in arg0 String The url that the worker should fetch
	 * @out String The HTML from the webpage at arg0
	 */
	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("arg0")) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}
		
		String link = (String)task.getParam("arg0");
		try {
			URL url = new URL(link);
	        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
	        String line = null;
	        StringBuffer webPage = new StringBuffer();
	
	        while ((line = reader.readLine()) != null) {
	        	webPage.append(line + "\n");
	        }
	        reader.close();
	        
	        Task newTask = new Task(task, this.getNextWorker());
	        newTask.addParam("arg0", webPage.toString());
	        
	        result.addNextTask(newTask);
	        result.setResult(TaskResult.Result.SUCCESS);
		} catch (IOException e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		}
		
		return result;
	}

}
