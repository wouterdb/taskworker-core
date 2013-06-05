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

package drm.taskworker.queue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;

import drm.taskworker.Entities;
import drm.util.Triplet;

/**
 * 
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Queue {

	private Keyspace cs;
	private Map<String, ColumnPrefixDistributedRowLock<String>> locks = new HashMap<>();

	public Queue(String queueName) {
		cs = Entities.cs();
	}
	
	private ColumnPrefixDistributedRowLock<String> getLock(String lockName) {
		if (this.locks.containsKey(lockName)) {
			return this.locks.get(lockName);
		}
		ColumnPrefixDistributedRowLock<String> lock = new ColumnPrefixDistributedRowLock<String>(this.cs, 
				Entities.CF_STANDARD1, lockName)
			.withBackoff(new BoundedExponentialBackoff(250, 10000, 10))
	        .expireLockAfter(1, TimeUnit.SECONDS);
		
		this.locks.put(lockName, lock);
		
		return lock; 
	}
	
	private boolean lock(String workerName, UUID workflowID) {
		String lockName = workerName + workflowID.toString();
		ColumnPrefixDistributedRowLock<String> lock = this.getLock(lockName); 
		try {
			lock.acquire();
		 } catch (Exception e) {
			 e.printStackTrace();
			 return false;
		 }
		return true;
	}
	
	private boolean release(String workerName, UUID workflowID) {
		String lockName = workerName + workflowID.toString();
		ColumnPrefixDistributedRowLock<String> lock = this.getLock(lockName); 
		try {
			lock.release();
		 } catch (Exception e) {
			 return false;
		 }
		return true;
	}

//	/**
//	 * assume saved
//	 * 
//	 * Add a new task to the queue.
//	 */
//	public void add(AbstractTask task) {
//		try {
//			queue(task);
//
//		} catch (ConnectionException e) {
//			throw new RuntimeException(e);
//		}
//	}
//
//	private void queue(AbstractTask task) throws ConnectionException {
//		PreparedCqlQuery<String, String> doQueue = cs
//				.prepareQuery(Entities.CF_STANDARD1)
//				.withCql(
//						"INSERT INTO taskqueue (id, workflow_id, worker_name, end) VALUES (?, ?,?,?);")
//				.asPreparedStatement();
//		doQueue.withUUIDValue(task.getId()).withUUIDValue(task.getWorkflowId())
//				.withStringValue(task.getWorker())
//				.withBooleanValue(task instanceof EndTask).execute();
//	}

	/**
	 * Lease a task from the queue
	 * 
	 * @param lease
	 *            How long is the task leased
	 * @param unit
	 *            The timeunit of the lease time
	 * @param limit
	 *            How many task are to be leased
	 * @param taskType
	 *            The type of task (the name / type of the worker)
	 * @param workflowId
	 *            The workflow the task belongs to
	 * @return A list of taskhandles 
	 * @throws ConnectionException
	 */
	public List<Triplet<UUID, UUID, String>> leaseTasks(long lease,
			TimeUnit unit, int limit, String taskType, UUID workflowId)
			throws ConnectionException {
		
		List<Triplet<UUID, UUID, String>> handles = new LinkedList<Triplet<UUID, UUID, String>>();
		
		if (workflowId == null) {
			return handles;
		}
		
		// get a lock
		if (!this.lock(taskType, workflowId)) {
			System.err.println("no lock, kaka");
			System.exit(1);
			return null;
		}
		
		long leaseSeconds = unit.toSeconds(lease);
		
		// try to dequeue a task (type = 0 and state = 0)
		PreparedCqlQuery<String, String> findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM tasks WHERE workflow_id = ? AND worker_name = ? AND type = 0 AND state = 1 AND LIMIT ?")
				.asPreparedStatement();
		
		CqlResult<String, String> result = findTasks
				.withUUIDValue(workflowId)
				.withStringValue(taskType)
				.withIntegerValue(limit)
				.execute().getResult();
		
		if (result.getNumber() > 0) {
			for (Row<String, String> row : result.getRows()) {	
				ColumnList<String> c = row.getColumns();
				
				Triplet<UUID, UUID, String> record = new Triplet<UUID, UUID, String>(workflowId, c.getUUIDValue("id", null), taskType);
				handles.add(record);
			}
			
		} else {
			// check if all tasks are finished, if so, check the end task
			findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM tasks WHERE workflow_id = ? AND worker_name = ? AND type = 0 AND state < 3")
					.asPreparedStatement();
			
			result = findTasks
					.withUUIDValue(workflowId)
					.withStringValue(taskType)
					.execute().getResult();
			
			if (result.getNumber() == 0) {
				// try to dequeue an endtask
				findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
						.withCql("SELECT * FROM tasks WHERE workflow_id = ? AND worker_name = ? AND state = 1 AND type == 1")
						.asPreparedStatement();
				
				result = findTasks.withUUIDValue(workflowId)
						.withStringValue(taskType).execute().getResult();
				
				assert(result.getNumber() <= 1); // only one end task can exist
				if (result.getNumber() > 0) {
					for (Row<String, String> row : result.getRows()) {	
						ColumnList<String> c = row.getColumns();
						Triplet<UUID, UUID, String> record = new Triplet<UUID, UUID, String>(workflowId, c.getUUIDValue("id", null), taskType);
						handles.add(record);
					}
				}
			}
		}
		
		// build a batch query that "leases" the tasks
		String query = "BATCH\n";
		for (Triplet<UUID, UUID, String> record : handles) {
			query += "  UPDATE task SET state = 2 USING TTL " + leaseSeconds + " WHERE workflow_id = " + 
					record.getA().toString() + " AND id = " + 
					record.getB().toString() + " AND type = '" + record.getC() + "';\n";
		}
		query += "APPLY BATCH;\n";
		cs.prepareQuery(Entities.CF_STANDARD1).withCql(query).execute();

		// release the lock
		if (!this.release(taskType, workflowId)) {
			// failed to release the lock, CRAP
			System.err.println("no release, kaka");
			return null;
		}

		return handles;
	}

}
