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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.EndTask;
import drm.taskworker.Task;
import drm.taskworker.TaskResult;
import drm.taskworker.Worker;

/**
 * @author bart
 *
 */
public class CSVInvoiceWorker extends Worker {
	public static final String NEXT_TASK = "template-invoice";
	
	private MemcacheService cacheService = MemcacheServiceFactory.getMemcacheService();

	public CSVInvoiceWorker() {
		super("csv-invoice");
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("cacheKey")) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}
		
		String csv_data = (String)cacheService.get(task.getParam("cacheKey"));
		
		// read in the csv
		try {
			CSVReader parser = new CSVReader(new StringReader(csv_data), ';');
			List<String[]> rows = parser.readAll();
			String[] headers = rows.get(0);
			
			for (int i = 1; i < rows.size(); i++) {
				String[] row = rows.get(i);
				Task newTask = new Task(NEXT_TASK);

				for (int j = 0; j < row.length; j++) {
					newTask.addParam(headers[j], row[j]);
				}
				
				result.addNextTask(newTask);
			}
			parser.close();
			
			result.setResult(TaskResult.Result.SUCCESS);
		} catch (FileNotFoundException e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		} catch (IOException e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		}
		
		return result;
	}

	@Override
	public TaskResult work(EndTask task) {
		TaskResult result = new TaskResult();
		result.addNextTask(new EndTask(NEXT_TASK));
		
		return result.setResult(TaskResult.Result.SUCCESS);
	}

}
