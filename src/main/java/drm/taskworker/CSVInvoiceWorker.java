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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author bart
 *
 */
public class CSVInvoiceWorker extends Worker {

	public CSVInvoiceWorker() {
		super("csv-invoice");
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("csv-input")) {
			return result.setResult(TaskResult.Result.ERROR);
		}
		
		System.out.println("Getting csv data");
		String csv_data = (String) task.getParam("csv-input");
		System.err.println(csv_data);
		
		// read in the csv
		try {
			System.out.println("Parsing csv data");
			CSVReader parser = new CSVReader(new StringReader(csv_data), ';');
			List<String[]> rows = parser.readAll();
			String[] headers = rows.get(0);
			
			System.out.println("Generating invoice tasks: " + rows.size());
			for (int i = 1; i < rows.size(); i++) {
				System.out.println(" invoice " + i);
				String[] row = rows.get(i);
				Task newTask = new Task("tex-invoice");

				for (int j = 0; j < row.length; j++) {
					newTask.addParam(headers[j], row[j]);
				}
				
				result.addNextTask(newTask);
			}
			parser.close();
			
			result.setResult(TaskResult.Result.SUCCESS);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}

}
