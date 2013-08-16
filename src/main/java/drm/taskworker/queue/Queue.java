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

import com.codahale.metrics.Timer.Context;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;

import drm.taskworker.Entities;
import drm.taskworker.config.Config;
import drm.taskworker.monitoring.Metrics;
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
	
	private String lockName(String workerName, UUID workflowID) {
		return workerName + "-" + workflowID.toString();
	}
	
	private boolean lock(String workerName, UUID workflowID) {
		Context tc = Metrics.timer("queue.lock").time();
		
		String lockName = this.lockName(workerName, workflowID);
		ColumnPrefixDistributedRowLock<String> lock = this.getLock(lockName); 
		try {
			lock.acquire();
		 } catch (Exception e) {
			 logger.log(Level.SEVERE, "Unable to aquire lock", e);
			 e.printStackTrace();
			 return false;
		 } finally {
			 tc.stop();
		 }
		return true;
	}
	
	private boolean release(String workerName, UUID workflowID) {
		Context tc = Metrics.timer("queue.unlock").time();
		
		String lockName = this.lockName(workerName, workflowID);
		ColumnPrefixDistributedRowLock<String> lock = this.getLock(lockName); 
		try {
			lock.release();
		 } catch (Exception e) {
			 logger.log(Level.SEVERE, "Unable to release lock", e);
			 return false;
		 } finally {
			 tc.stop();
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
	public synchronized List<TaskHandle> leaseTasks(long lease,
			TimeUnit unit, int limit, String taskType, UUID workflowId)
			throws ConnectionException {
		
		if (workflowId == null) {
			return new LinkedList<>();
		}
		
		List<TaskHandle> handles = null;
		
		boolean distributed = Boolean.parseBoolean(Config.getConfig().getProperty("dreamaas.distributed"));
		
		// get a lock if distributed is true
		if (distributed && !this.lock(taskType, workflowId)) {
			logger.warning("Unable to aquire a lock on " + workflowId.toString() + " - " + taskType);
			return handles;
		}

		Context tc = Metrics.timer("queue.lease").time();
		handles = leaseWithNoLock(lease, unit, limit, taskType, workflowId);
		tc.stop();

		// release the lock if distributed true
		if (distributed && !this.release(taskType, workflowId)) {
			logger.warning("Unable to release lock on " + workflowId.toString() + " - " + taskType);
			return null;
		}

		return handles;
	}
	
	private boolean handleExpiredLease(TaskHandle task) throws ConnectionException{
		// check if the task has a timing entry which also marks a task as
		// finished. If so, ignore the task and try to do yet an other delete.
		PreparedCqlQuery<String, String> findTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql("SELECT * FROM task_timing WHERE id = ?")
				.asPreparedStatement();
		
		CqlResult<String, String> result = findTask
				.withUUIDValue(task.getId())
				.execute().getResult();
		Rows<String, String> rows = result.getRows();
		
		if (rows.size() > 0) {
			// the task did finish, try to mark it as removed once again
			PreparedCqlQuery<String, String> markDelete = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL)
					.withCql("UPDATE task_queue SET removed = true WHERE queue_id = ? AND type = ? AND id = ?")
					.asPreparedStatement();
			
			markDelete
					.withStringValue(this.lockName(task.getWorkerName(), task.getJobID()))
					.withIntegerValue(task.getType())
					.withUUIDValue(task.getId())
					.execute().getResult();
			
			return false;
		} else {
			// this is a real expired
			logger.warning("Leasing task " + task.getId() + " from which the lease expired.");
			return true;
		}
	}

	/**
	 * Lease tasks without requesting a lock
	 * 
	 * @pre requires a lock
	 * @see Queue#leaseTasks(long, TimeUnit, int, String, UUID)
	 */
	private List<TaskHandle> leaseWithNoLock(long lease, TimeUnit unit, int limit,
			String taskType, UUID workflowId) throws ConnectionException {
		List<TaskHandle> handles = new LinkedList<>();
		
		if (workflowId == null) {
			return handles;
		}
		
		if (limit != 1) {
			throw new IllegalArgumentException("Limit more than one not supported!");
		}
		
		long leaseSeconds = unit.toMillis(lease);
		long now = System.currentTimeMillis();
		
		// try to dequeue a task (type = 0 and lease = false)
		PreparedCqlQuery<String, String> findTasks = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql("SELECT * FROM task_queue WHERE queue_id = ?")
				.asPreparedStatement();
		
		CqlResult<String, String> result = findTasks
				.withStringValue(this.lockName(taskType, workflowId))
				.execute().getResult();
		Rows<String, String> rows = result.getRows();
		
		for (Row<String, String> row : rows) {	
			ColumnList<String> c = row.getColumns();

			// add all work tasks that do not have a lease on them
			if (c.getBooleanValue("removed", null) == null || c.getBooleanValue("removed", null)) {
				// ignore, strange cassandra crap going on
			} else {
				long leased_until = c.getColumnByName("leased_until").getLongValue();

				if (leased_until < now && c.getColumnByName("type").getIntegerValue() == 0) {
					TaskHandle handle = new TaskHandle();
					handle.setJobID(workflowId);
					handle.setId(c.getUUIDValue("id", null));
					handle.setWorkerName(taskType);
					handle.setType(0);
					
					if (leased_until > 0 && leased_until < now) {
						// HACK: check if it is a real expire
						if (this.handleExpiredLease(handle)) {
							handles.add(handle);
						}
					} else {
						handles.add(handle);
					}
				}
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

				if (c.getBooleanValue("removed", null) == null || c.getBooleanValue("removed", null)) {
					// ignore, strange cassandra crap going on
					continue;
				}
				
				int type = c.getColumnByName("type").getIntegerValue();
				long leased_until = c.getColumnByName("leased_until").getLongValue();
				// count the number of leased normal tasks or create endtasks
				if (type == 0) {
					if (leased_until > now) {
						n++;
					}
				} else if (type == 1)  {
					if (leased_until < now) {
						endTask = new TaskHandle();
						endTask.setJobID(workflowId);
						endTask.setId(c.getUUIDValue("id", null));
						endTask.setType(1);
						endTask.setWorkerName(taskType);
						
						if (leased_until > 0 && leased_until < now) {
							// HACK: check if it is a real expire
							if (!this.handleExpiredLease(endTask)) {
								endTask = null;
							}
						}
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
//			String query = "BEGIN BATCH\n";
//			for (TaskHandle handle : handles) {
//				query += "  UPDATE task_queue SET leased_until = " + (now + leaseSeconds) + ", removed = false WHERE queue_id = '" + 
//						this.lockName(handle.getWorkerName(), handle.getWorkflowID()) 
//						+ "' AND type = " + handle.getType() + " AND id = " + handle.getId() + ";\n";
//				
//				logger.info("Leasing task " + handle.getId() + " " + handle.getType());
//			}
//			query += "APPLY BATCH;\n";
//			
//			cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM).withCql(query).execute();
			
			for (TaskHandle handle : handles) {
				PreparedCqlQuery<String, String> leaseTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
						.withCql("UPDATE task_queue SET leased_until = ?, removed = false WHERE queue_id = ? AND type = ? AND id = ?")
						.asPreparedStatement();
				
				result = leaseTask
						.withLongValue(now + leaseSeconds)
						.withStringValue(this.lockName(handle.getWorkerName(), handle.getJobID()))
						.withIntegerValue(handle.getType())
						.withUUIDValue(handle.getId())
						.execute().getResult();
			}
		}
		
		return handles;
	}

	/**
	 * Remove a task from the queue
	 */
	public void finishTask(AbstractTask task) {
		Context tc = Metrics.timer("queue.finish").time();
		// TODO: ensure that we still have a lease here
		logger.info("Removing task " + task.getId() + " " + task.getTaskType());
		try {
			PreparedCqlQuery<String, String> markDelete = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL)
					.withCql("UPDATE task_queue SET removed = true WHERE queue_id = ? AND type = ? AND id = ?")
					.asPreparedStatement();
			
			markDelete
					.withStringValue(this.lockName(task.getWorker(), task.getJobId()))
					.withIntegerValue(task.getTaskType())
					.withUUIDValue(task.getId())
					.execute().getResult();
			
			if (task.getTaskType() == 1) {
				// this is an end-task, to we can go ahead and remove the entire "queue" row
				PreparedCqlQuery<String, String> deleteTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
					.withCql("DELETE FROM task_queue WHERE queue_id = ?")
					.asPreparedStatement();
			
				deleteTask
					.withStringValue(this.lockName(task.getWorker(), task.getJobId()))
					.execute().getResult();
			}
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		} finally {
			tc.stop();
		}
	}

	/**
	 * Add a task to the queue
	 */
	public void addTask(AbstractTask task) {
		Context tc = Metrics.timer("queue.addtask").time();
		logger.info("Inserting task " + task.getId() + " " + task.getTaskType());
		try {
			PreparedCqlQuery<String, String> addTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
					.withCql("INSERT INTO task_queue (queue_id, type, id, leased_until, removed) VALUES (?, ?, ?, 0, false)")
					.asPreparedStatement();
			
			addTask
					.withStringValue(this.lockName(task.getWorker(),  task.getJobId()))
					.withIntegerValue(task.getTaskType())
					.withUUIDValue(task.getId())
					.execute().getResult();
			
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		} finally {
			tc.stop();
		}
	}

}
