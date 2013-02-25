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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

import drm.taskworker.EndTask;
import drm.taskworker.Task;
import drm.taskworker.TaskResult;
import drm.taskworker.Worker;

public class PdfRenderWorker extends Worker {
	public PdfRenderWorker() {
		super("pdf-render");
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("invoice-source")) {
			return result.setResult(TaskResult.Result.ERROR);
		}
		
		try {
			File texFile = File.createTempFile("builder-", ".tex");
			FileWriter writer = new FileWriter(texFile);
			writer.write((String)task.getParam("invoice-source"));
			writer.close();

			Process p = Runtime.getRuntime().exec(
					"rubber --into=/tmp -d " + texFile.toString());
			p.waitFor();

			String path = texFile.toString();
			String prefix = path.substring(0, path.length() - 4);

			File pdfFile = new File(prefix + ".pdf");
			byte[] fileData = new byte[(int) pdfFile.length()];
			DataInputStream dis = new DataInputStream(new FileInputStream(pdfFile));
			dis.readFully(fileData);
			dis.close();
			
			// clean up
			pdfFile.delete();
			texFile.delete();
			File logFile = new File(prefix + ".log");
			logFile.delete();
			File auxFile = new File(prefix + ".aux");
			auxFile.delete();
			
//			Task newTask = new Task("zip-files");
//			newTask.addParam("file", fileData);
//			result.addNextTask(newTask);
			result.setResult(TaskResult.Result.SUCCESS);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public TaskResult work(EndTask task) {
		// TODO Auto-generated method stub
		return null;
	}
}
