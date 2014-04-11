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
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.BoundedExponentialBackoff;
import dnet.minimetrics.TimerContext;

import drm.taskworker.Entities;
import drm.taskworker.config.Config;
import drm.taskworker.monitoring.Metrics;
import drm.taskworker.tasks.Task;

/**
 * A queue implementation on top of cassandra
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Queue {
	private static Logger logger = Logger.getLogger(Queue.class.getCanonicalName());

	private Keyspace cs;
	private Map<String, ColumnPrefixDistributedRowLock<String>> locks = new HashMap<>();
	private Map<String, Object> localLocks = new HashMap<>(); // TODO remove stale entries
	
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
		TimerContext tc = Metrics.timer("queue.lock").time();
		
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
		TimerContext tc = Metrics.timer("queue.unlock").time();
		
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
	 * @param jobId
	 *            The workflow the task belongs to
	 * @return A list of taskhandles 
	 * @throws ConnectionException
	 */
	public List<TaskHandle> leaseTasks(long lease,
			TimeUnit unit, int limit, String taskType, UUID jobId)
			throws ConnectionException {
		
		if (jobId == null) {
			return new LinkedList<>();
		}
		
		List<TaskHandle> handles = null;
		
		boolean distributed = Boolean.parseBoolean(Config.getConfig().getProperty("taskworker.distributed"));
		String lockKey = this.lockName(taskType, jobId);
		Object lockObject = null;
		
		if (!this.localLocks.containsKey(lockKey)) {
			synchronized (this.localLocks) {
				if (!this.localLocks.containsKey(lockKey)) {
					lockObject = new Object();
					this.localLocks.put(lockKey, lockObject);
				}else{
					lockObject = this.localLocks.get(lockKey);
				}
			}
		} else {
			lockObject = this.localLocks.get(lockKey);
		}

		//
		synchronized (lockObject) { // first do a local lock, because the class is shared between all threads
			// get a lock if distributed is true
			if (distributed && !this.lock(taskType, jobId)) {
				logger.warning("Unable to aquire a lock on " + jobId.toString() + " - " + taskType);
				return handles;
			}
	
			TimerContext tc = Metrics.timer("queue.lease").time();
			handles = leaseWithNoLock(lease, unit, limit, taskType, jobId);
			tc.stop();
	
			// release the lock if distributed true
			if (distributed && !this.release(taskType, jobId)) {
				logger.warning("Unable to release lock on " + jobId.toString() + " - " + taskType);
				return null;
			}
		}

		return handles;
	}
	
	private boolean handleExpiredLease(TaskHandle task) throws ConnectionException{
		// check if the task has a timing entry which also marks a task as
		// finished. If so, ignore the task and try to do yet an other delete.
		PreparedCqlQuery<String, String> findTask = cs.prepareQuery(Entities.CF_STANDARD1)
				.setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql("SELECT * FROM task_timing WHERE id = ?")
				.asPreparedStatement();
		
		CqlResult<String, String> result = findTask
				.withUUIDValue(task.getId())
				.execute().getResult();
		Rows<String, String> rows = result.getRows();
		
		if (rows.size() > 0) {
			// the task did finish, try to mark it as removed once again
			PreparedCqlQuery<String, String> markDelete = cs.prepareQuery(Entities.CF_STANDARD1)
					.setConsistencyLevel(ConsistencyLevel.CL_ALL)
					.withCql("UPDATE task_queue SET removed = true WHERE queue_id = ? AND id = ?")
					.asPreparedStatement();
			
			markDelete
					.withStringValue(this.lockName(task.getWorkerName(), task.getJobID()))
					.withUUIDValue(task.getId())
					.execute().getResult();
			
			PreparedCqlQuery<String, String> deleteTask = cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("DELETE FROM task_queue WHERE queue_id = ? AND id = ?")
					.asPreparedStatement();
			
			deleteTask
					.withStringValue(this.lockName(task.getWorkerName(), task.getJobID()))
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
		PreparedCqlQuery<String, String> findTasks = cs.prepareQuery(Entities.CF_STANDARD1)
				.setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
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
				
				UUID taskid = c.getUUIDValue("id", null);
				
				removeFromQueue(taskType, workflowId, taskid);
				
			} else {
				long leased_until = c.getLongValue("leased_until", 0L);

				if (leased_until < now) {
					TaskHandle handle = new TaskHandle();
					handle.setJobID(workflowId);
					handle.setId(c.getUUIDValue("id", null));
					handle.setWorkerName(taskType);
					
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
		
		// build a batch query that "leases" the tasks
		if (handles.size() > 0) {
			for (TaskHandle handle : handles) {
				PreparedCqlQuery<String, String> leaseTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
						.withCql("UPDATE task_queue SET leased_until = ?, removed = false WHERE queue_id = ? AND id = ?")
						.asPreparedStatement();
				
				result = leaseTask
						.withLongValue(now + leaseSeconds)
						.withStringValue(this.lockName(handle.getWorkerName(), handle.getJobID()))
						.withUUIDValue(handle.getId())
						.execute().getResult();
			}
		}
		
		return handles;
	}

	private void removeFromQueue(String taskType, UUID workflowId, UUID taskid)
			throws ConnectionException {
		PreparedCqlQuery<String, String> deleteTask = cs.prepareQuery(Entities.CF_STANDARD1)
				.withCql("DELETE FROM task_queue WHERE queue_id = ? AND id = ?")
				.asPreparedStatement();
		
		deleteTask
				.withStringValue(this.lockName(taskType, workflowId))
				.withUUIDValue(taskid)
				.execute().getResult();
	}

	/**
	 * Remove a task from the queue
	 */
	public void finishTask(Task task) {
		TimerContext tc = Metrics.timer("queue.finish").time();
		// TODO: ensure that we still have a lease here
		logger.info("Removing task " + task.getId());
		try {
			PreparedCqlQuery<String, String> markDelete = cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE task_queue SET removed = true WHERE queue_id = ? AND id = ?")
					.asPreparedStatement();
			
			markDelete.withStringValue(this.lockName(task.getWorker(), task.getJobId()))
					.withUUIDValue(task.getId())
					.execute().getResult();
			
			PreparedCqlQuery<String, String> deleteTask = cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("DELETE FROM task_queue WHERE queue_id = ? AND id = ?")
					.asPreparedStatement();
			
			deleteTask
					.withStringValue(this.lockName(task.getWorker(), task.getJobId()))
					.withUUIDValue(task.getId())
					.execute().getResult();
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		} finally {
			tc.stop();
		}
	}

	/**
	 * Add a task to the queue
	 */
	public void addTask(Task task) {
		TimerContext tc = Metrics.timer("queue.addtask").time();
		logger.info("Inserting task " + task.getId());
		try {
			PreparedCqlQuery<String, String> addTask = cs.prepareQuery(Entities.CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
					.withCql("INSERT INTO task_queue (queue_id, id, leased_until, removed) VALUES (?, ?, 0, false)")
					.asPreparedStatement();
			
			addTask
					.withStringValue(this.lockName(task.getWorker(),  task.getJobId()))
					.withUUIDValue(task.getId())
					.execute().getResult();
			
		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		} finally {
			tc.stop();
		}
	}

}
