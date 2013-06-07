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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;

import drm.taskworker.Entities;
import drm.taskworker.tasks.AbstractTask;

/**
 * A queue implementation on top of cassandra
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Queue {
	private static Logger logger = Logger.getLogger(Queue.class.getCanonicalName());

	private Keyspace cs;
	private Map<String, ColumnPrefixDistributedRowLock<String>> locks = new HashMap<>();

	public Queue(String queueName) {
		cs = Entities.cs();
	}
	
	private ColumnPrefixDistributedRowLock<String> getLock(String lockName) {
		if (this.locks.containsKey(lockName)) {
			return this.locks.get(lockName);
		}
		ColumnPrefixDistributedRowLock<String> lock = new ColumnPrefixDistributedRowLock<String>(this.cs, Entities.CF_STANDARD1, lockName)
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
			 logger.log(Level.SEVERE, "Unable to aquire lock", e);
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
			 logger.log(Level.SEVERE, "Unable to release lock", e);
			 return false;
		 }
		return true;
	}

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
	public List<TaskHandle> leaseTasks(long lease,
			TimeUnit unit, int limit, String taskType, UUID workflowId)
			throws ConnectionException {
		
		if (workflowId == null) {
			return new LinkedList<>();
		}
		
		List<TaskHandle> handles = null;
		
		boolean distributed = Boolean.parseBoolean(System.getProperty("dreamaas.distributed"));
		
		if (distributed) {
			// get a lock
			if (!this.lock(taskType, workflowId)) {
				logger.warning("Unable to aquire a lock on " + workflowId.toString() + " - " + taskType);
				return handles;
			}
				
			handles = leaseWithNoLock(lease, unit, limit, taskType, workflowId);
			
			// release the lock
			if (!this.release(taskType, workflowId)) {
				logger.warning("Unable to release lock on " + workflowId.toString() + " - " + taskType);
				return null;
			}
		} else {
			// use synchronised
			synchronized (this.cs) {
				handles = leaseWithNoLock(lease, unit, limit, taskType, workflowId);
			}
		}

		return handles;
	}

	/**
	 * Lease tasks
	 * 
	 * @see Queue#leaseTasks(long, TimeUnit, int, String, UUID)
	 */
	private List<TaskHandle> leaseWithNoLock(long lease, TimeUnit unit, int limit,
			String taskType, UUID workflowId) throws ConnectionException {
		List<TaskHandle> handles = new LinkedList<>();
		
		if (workflowId == null) {
			return handles;
		}
		
		long leaseSeconds = unit.toMillis(lease);
		long now = System.currentTimeMillis();
		
		// try to dequeue a task (type = 0 and lease = false)
		PreparedCqlQuery<String, String> findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM task_queue WHERE workflow_id = ? AND worker_name = ?")
				.asPreparedStatement();
		
		CqlResult<String, String> result = findTasks
				.withUUIDValue(workflowId)
				.withStringValue(taskType)
				.execute().getResult();
		Rows<String, String> rows = result.getRows();
		
		for (Row<String, String> row : rows) {	
			ColumnList<String> c = row.getColumns();
			
			// add all work tasks that do not have a lease on them
			if (c.getBooleanValue("removed", false)) {
				// ignore
				
			} else if (c.getColumnByName("leased_until").getLongValue() < now && 
					c.getColumnByName("type").getIntegerValue() == 0) {
				TaskHandle handle = new TaskHandle();
				handle.setWorkflowID(workflowId);
				handle.setId(c.getUUIDValue("id", null));
				handle.setWorkerName(taskType);
				handle.setType(0);
				handles.add(handle);
			}
			
			// stop searching when we reach the limit
			if (handles.size() == limit) {
				break;
			}
		}
		
		// if we selected no tasks, check if we can lease an end task
		// TODO: this can all be move in the loop above
		if (handles.size() == 0) {
			TaskHandle endTask = null;
			// count the number of running (leased) normal tasks
			int n = 0;
			for (Row<String, String> row : result.getRows()) {
				ColumnList<String> c = row.getColumns();
				int type = c.getColumnByName("type").getIntegerValue();
				if (type == 0) {
					if (c.getColumnByName("leased_until").getLongValue() > now) {
						n++;
					}
				} else if (type == 1)  {
					if (c.getColumnByName("leased_until").getLongValue() < now) {
						endTask = new TaskHandle();
						endTask.setWorkflowID(workflowId);
						endTask.setId(c.getUUIDValue("id", null));
						endTask.setType(1);
						endTask.setWorkerName(taskType);
					} else {
						// the end task has been leased, so for now we are out
						// of work
					}
				}
			}
			
			// if no leased normal tasks are left, return the endtask
			if (n == 0 && endTask != null) {
				handles.add(endTask);
			}
		}
		
		// build a batch query that "leases" the tasks
		if (handles.size() > 0) {
			String query = "BEGIN BATCH\n";
			for (TaskHandle handle : handles) {
				query += "  UPDATE task_queue SET leased_until = " + (now + leaseSeconds) + " WHERE workflow_id = " + 
						handle.getWorkflowID() + " AND worker_name = '" + 
						handle.getWorkerName() + "' AND type = " + handle.getType() + 
						" AND id = " + handle.getId() + ";\n";
				
				logger.info("Leasing task " + handle.getId() + " " + handle.getType());
			}
			query += "APPLY BATCH;\n";
			
			cs.prepareQuery(Entities.CF_STANDARD1).withCql(query).execute();
		}
		
		return handles;
	}

	/**
	 * Remove a task from the queue
	 */
	public void finishTask(AbstractTask task) {
		// TODO: ensure that we still have a lease here
		logger.info("Removing task " + task.getId() + " " + task.getTaskType());
		try {
			String update_ql = "UPDATE task_queue SET removed = true WHERE workflow_id = " + 
					task.getWorkflowId() + " AND worker_name = '" + 
					task.getWorker() + "' AND type = " +
					task.getTaskType() + " AND id = " + task.getId() + ";\n";
			
			String delete_ql = "DELETE FROM task_queue WHERE workflow_id = " + 
					task.getWorkflowId() + " AND worker_name = '" + 
					task.getWorker() + "' AND type = " +
					task.getTaskType() + " AND id = " + task.getId() + ";\n";
			
			String query = "BEGIN BATCH\n" + update_ql + delete_ql + "APPLY BATCH;\n";
			
			cs.prepareQuery(Entities.CF_STANDARD1).withCql(query).execute();
			
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Add a task to the queue
	 */
	public void addTask(AbstractTask task) {
		logger.info("Inserting task " + task.getId() + " " + task.getTaskType());
		try {
			PreparedCqlQuery<String, String> findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("INSERT INTO task_queue (workflow_id, worker_name, type, id, leased_until, removed) VALUES (?, ?, ?, ?, 0, false)")
					.asPreparedStatement();
			
			findTasks
					.withUUIDValue(task.getWorkflowId())
					.withStringValue(task.getWorker())
					.withIntegerValue(task.getTaskType())
					.withUUIDValue(task.getId())
					.execute().getResult();
			
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		}
	}

}
