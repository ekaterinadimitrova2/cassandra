/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.when;

public class MemtableCleanerThreadTest
{
    private static final long TIMEOUT_SECONDS = 5;
    private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);

    private static final double epsilon = 0.000001;

    @Mock
    MemtablePool pool;

    @Mock
    MemtableCleaner cleaner;

    MemtableCleanerThread cleanerThread;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    private void startThread(int maxPendingTasks, double cleanThreshold)
    {
        cleanerThread = new MemtableCleanerThread(pool, cleaner, maxPendingTasks, cleanThreshold);
        assertNotNull(cleanerThread);
        cleanerThread.start();

        for (int i = 0; i < TIMEOUT_MILLIS && !cleanerThread.isAlive(); i++)
            FBUtilities.sleepQuietly(1);

        assertTrue(cleanerThread.isAlive());
    }

    private void stopThread()
    {
        cleanerThread.shutdownNow();

        for (int i = 0; i < TIMEOUT_MILLIS && cleanerThread.isAlive(); i++)
            FBUtilities.sleepQuietly(1);

        assertFalse(cleanerThread.isAlive());
    }

    private void waitForPendingTasks(int numPending)
    {
        // wait for a bit because the cleaner latch completes before the pending tasks is decremented
        FBUtilities.sleepQuietly(1);

        for (int i = 0; i < TIMEOUT_MILLIS && cleanerThread.numPendingTasks() != numPending; i++)
            FBUtilities.sleepQuietly(1);

        assertEquals(numPending, cleanerThread.numPendingTasks());
    }

    @Test
    public void testCleanerInvoked() throws Exception
    {
        final int maxPendingTasks = 2;
        final double cleanThreshold = 0.2;
        final AtomicReference<Double> ownershipRatio = new AtomicReference<>();
        CountDownLatch cleanerExecutedLatch = new CountDownLatch(1);
        CompletableFuture fut = new CompletableFuture();
        AtomicBoolean needsCleaning = new AtomicBoolean(false);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        when(cleaner.clean(anyDouble())).thenAnswer(invocation -> {
            ownershipRatio.set(invocation.getArgument(0));
            needsCleaning.set(false);
            cleanerExecutedLatch.countDown();
            return fut;
        });

        // start the thread with needsCleaning returning false, the cleaner should not be invoked
        needsCleaning.set(false);
        startThread(maxPendingTasks, cleanThreshold);
        cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, cleanerExecutedLatch.getCount());
        assertEquals(0, cleanerThread.numPendingTasks());

        // now invoke the cleaner, it should receive an ownership ratio of zero because there are < maxPendingTasks
        needsCleaning.set(true);
        cleanerThread.maybeClean();
        cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerExecutedLatch.getCount());
        assertEquals(0., ownershipRatio.get(), epsilon);
        assertEquals(1, cleanerThread.numPendingTasks());

        // now complete the cleaning task
        needsCleaning.set(false);
        fut.complete(true);
        waitForPendingTasks(0);

        stopThread();
    }

    @Test
    public void testCleanThreshold() throws Exception
    {
        final int maxPendingTasks = 1;
        final double cleanThreshold = 0.2;
        AtomicReference<CountDownLatch> cleanerLatch = new AtomicReference<>(new CountDownLatch(1));
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(2);
        List<Double> ownerhsipRatio = new ArrayList<>(2);
        AtomicBoolean needsCleaning = new AtomicBoolean(false);

        when(cleaner.clean(anyDouble())).thenAnswer(invocation -> {
            CompletableFuture fut = new CompletableFuture();
            futures.add(fut);
            ownerhsipRatio.add(invocation.getArgument(0));
            needsCleaning.set(false);
            cleanerLatch.get().countDown();
            return fut;
        });

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        needsCleaning.set(true);
        startThread(maxPendingTasks, cleanThreshold);

        // the thread started with needsCleaning returning true, the should be one pending task
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(1, cleanerThread.numPendingTasks());
        assertEquals(1, futures.size());
        assertEquals(1, ownerhsipRatio.size());
        assertEquals(0., ownerhsipRatio.get(0), epsilon);

        // trigger cleaning again, the second task should be invoked but with the clean threshold as the ownership ratio
        cleanerLatch.set(new CountDownLatch(1));
        needsCleaning.set(true);
        cleanerThread.maybeClean();
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(2, cleanerThread.numPendingTasks());
        assertEquals(2, futures.size());
        assertEquals(2, ownerhsipRatio.size());
        assertEquals(cleanThreshold, ownerhsipRatio.get(1), epsilon);

        // now complete the first task and make sure that the pending tasks are updated
        needsCleaning.set(false);
        futures.get(0).complete(true);
        waitForPendingTasks(1);

        // now complete the second task with needsCleaning() returning false and check no pending tasks
        needsCleaning.set(false);
        futures.get(1).complete(true);
        waitForPendingTasks(0);

        stopThread();
    }

    @Test
    public void testCleanerError() throws Exception
    {
        final int maxPendingTasks = 1;
        final double cleanThreshold = 0.2;
        AtomicReference<CountDownLatch> cleanerLatch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<CompletableFuture> fut = new AtomicReference<>(new CompletableFuture());
        AtomicReference<Double> ownerhsipRatio = new AtomicReference<>();
        AtomicBoolean needsCleaning = new AtomicBoolean(false);
        AtomicInteger numTimeCleanerInvoked = new AtomicInteger(0);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        when(cleaner.clean(anyDouble())).thenAnswer(invocation -> {
            ownerhsipRatio.set(invocation.getArgument(0));
            needsCleaning.set(false);
            numTimeCleanerInvoked.incrementAndGet();
            cleanerLatch.get().countDown();
            return fut.get();
        });

        // start the thread with needsCleaning returning true, the cleaner should be invoked
        // with a minimum ownership ratio of 0
        needsCleaning.set(true);
        startThread(maxPendingTasks, cleanThreshold);
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(1, cleanerThread.numPendingTasks());
        assertEquals(0, ownerhsipRatio.get(), epsilon);
        assertEquals(1, numTimeCleanerInvoked.get());

        // complete the cleaning task with an error, no other cleaning task should be invoked
        cleanerLatch.set(new CountDownLatch(1));
        ownerhsipRatio.set(null);
        CompletableFuture<Boolean> oldFut = fut.get();
        fut.set(new CompletableFuture());
        needsCleaning.set(false);
        oldFut.completeExceptionally(new RuntimeException("Test"));
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, cleanerLatch.get().getCount());
        assertEquals(1, numTimeCleanerInvoked.get());

        // now trigger cleaning again and verify that a new task is invoked
        cleanerLatch.set(new CountDownLatch(1));
        ownerhsipRatio.set(null);
        fut.set(new CompletableFuture());
        needsCleaning.set(true);
        cleanerThread.maybeClean();
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(2, numTimeCleanerInvoked.get());

        //  complete the cleaning task with false (nothing should be scheduled)
        cleanerLatch.set(new CountDownLatch(1));
        ownerhsipRatio.set(null);
        oldFut = fut.get();
        fut.set(new CompletableFuture());
        needsCleaning.set(false);
        oldFut.complete(false);
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(1, cleanerLatch.get().getCount());
        assertEquals(2, numTimeCleanerInvoked.get());

        // now trigger cleaning again and verify that a new task is invoked
        cleanerLatch.set(new CountDownLatch(1));
        ownerhsipRatio.set(null);
        fut.set(new CompletableFuture());
        needsCleaning.set(true);
        cleanerThread.maybeClean();
        cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertEquals(0, cleanerLatch.get().getCount());
        assertEquals(3, numTimeCleanerInvoked.get());

        stopThread();
    }
}
