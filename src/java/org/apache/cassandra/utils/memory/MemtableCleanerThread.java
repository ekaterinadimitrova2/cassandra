/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * A thread that reclaims memory from a MemtablePool on demand.  The actual reclaiming work is delegated to the
 * cleaner Runnable, e.g., FlushLargestColumnFamily
 */
class MemtableCleanerThread<P extends MemtablePool> extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableCleanerThread.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 2, TimeUnit.SECONDS);

    /** The pool we're cleaning */
    private final P pool;

    /** The asynchronous cleaner, see the documentation {@link MemtableCleaner} */
    private final MemtableCleaner cleaner;

    /** Signalled whenever needsCleaning() may return true */
    private final WaitQueue wait = new WaitQueue();

    /** This is incremented when a cleaner is invoked and decremented when a cleaner has completed */
    private final AtomicInteger numPendingTasks = new AtomicInteger(0);

    /** The maximum number of pending cleaning tasks, beyond this limit we pass the cleaner the threshold so that the cleaner
     * will not clean unless it has more than the threshold to clean. */
    private final int maxPendingTasks;

    /** The cleaning threshold, when we have more pending tasks than {@link this#maxPendingTasks}, then we pass this
     * threshold to the cleaner as a minimum memory to clean *.
     */
    private final double cleanThreshold;

    /** This is set to true when the cleaner should terminate */
    private volatile boolean stopRequested;

    MemtableCleanerThread(P pool, MemtableCleaner cleaner, int maxPendingTasks, double cleanThreshold)
    {
        super(pool.getClass().getSimpleName() + "Cleaner");
        this.pool = pool;
        this.cleaner = cleaner;
        this.maxPendingTasks = maxPendingTasks;
        this.cleanThreshold = cleanThreshold;
        this.stopRequested = false;

        setDaemon(true);

        logger.debug("Memtable cleaner created with {} max pending tasks", maxPendingTasks);
    }

    /**
     * Request that the thread stops executing
     */
    void requestStop()
    {
        this.stopRequested = true;
        maybeClean();
    }

    /**
     * This should be called by the pool when it has either increased its memory usage, or when it has reclaimed some memory.
     * The cleaner thread may launch a cleaner task as a result of calling this method, if {@link MemtablePool.SubPool#needsCleaning()}
     * returns true for at least one of the sub-pools (on or off heap) and if the number of ongoing cleaning tasks has not
     * exceeded the maximum.
     */
    void maybeClean()
    {
        wait.signal();
    }

    /** Return the number of pending tasks */
    int numPendingTasks()
    {
        return this.numPendingTasks.get();
    }

    @Override
    public void run()
    {
        while (!stopRequested)
        {
            while (!stopRequested && !pool.needsCleaning())
            {
                final WaitQueue.Signal signal = wait.register();
                if (!pool.needsCleaning() && !stopRequested)
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }

            if (stopRequested)
                break;

            // increment the number of pending tasks and determine the minimum ownership ratio to pass to the clener
            // this is zero (no min) if we have less than maxPendingTasks, otherwise it is the cleanThreshold
            // the reason is that we don't want to queue more than numPendingTasks, but at the same time we don't
            // want to end up with memtables that are too large, so if there is a memtable larger than cleanThreshold,
            // we want to flush that
            int numPendingTasks = this.numPendingTasks.incrementAndGet();
            double minOwnershipRatio = numPendingTasks > maxPendingTasks ? cleanThreshold : 0;

            if (logger.isTraceEnabled())
                logger.trace("Invoking cleaner with {} tasks pending", numPendingTasks);

            cleaner.clean(minOwnershipRatio).handle((res, err) -> {
                final int tasks = this.numPendingTasks.decrementAndGet();

                // if the cleaning job was scheduled (res == true) or had an error, trigger again after decrementing the tasks
                // Even if the pool also triggers when a the task completes (memtable reclaimed), this new trigger
                // may be able to reclaim more memory by passing a lower min ownership ratio because the pending tasks
                // have been decremented
                if ((res || err != null) && pool.needsCleaning())
                    maybeClean();

                if (err != null)
                    logger.error("Memtable cleaning tasks failed with an exception and {} pending tasks ",tasks, err);
                else if (logger.isTraceEnabled())
                    logger.trace("Memtable cleaning task completed ({}), currently pending: {}", res, tasks);

                return res;
            });

            // Pause for a bit in order to prevent this loop from spinning too much, should the quantities that are used by
            // pool.needsCleaning() not be updated (DB-4376)
            FBUtilities.sleepQuietly(1);
        }
    }
}
