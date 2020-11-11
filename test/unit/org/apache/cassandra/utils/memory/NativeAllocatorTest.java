/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.memory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NativeAllocatorTest
{
    ScheduledExecutorService exec;
    OpOrder order;
    OpOrder.Group group;
    CountDownLatch canClean;
    CountDownLatch isClean;
    AtomicReference<NativeAllocator> allocatorRef;
    AtomicReference<OpOrder.Barrier> barrier;
    NativePool pool;
    NativeAllocator allocator;
    Runnable markBlocking;

    @Before
    public void setUp()
    {
        exec = Executors.newScheduledThreadPool(2);
        order = new OpOrder();
        group = order.start();
        canClean = new CountDownLatch(1);
        isClean = new CountDownLatch(1);
        allocatorRef = new AtomicReference<>();
        barrier = new AtomicReference<>();
        pool = new NativePool(1, 100, 0.75f, (minOwnershipRatio) -> {
            try
            {
                canClean.await();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }
            if (isClean.getCount() > 0)
            {
                allocatorRef.get().offHeap().released(80);
                isClean.countDown();
            }

            return CompletableFuture.completedFuture(true);
        }, Integer.MAX_VALUE);
        allocator = new NativeAllocator(pool);
        allocatorRef.set(allocator);
        markBlocking = () -> {
            barrier.set(order.newBarrier());
            barrier.get().issue();
            barrier.get().markBlocking();
        };
    }

    private void verifyUsedReclaiming(long used, long reclaiming)
    {
        assertEquals(used, allocator.offHeap().owns());
        assertEquals(used, pool.offHeap.used());
        assertEquals(reclaiming, allocator.offHeap().getReclaiming());
        assertEquals(reclaiming, pool.offHeap.getReclaiming());
    }

    @Test
    public void testBookKeeping() throws ExecutionException, InterruptedException
    {
        final Runnable test = () -> {
            assertTrue(allocator.isLive());

            // allocate normal, check accounted and not cleaned
            allocator.allocate(10, group);
            verifyUsedReclaiming(10, 0);

            // confirm adjustment works
            allocator.offHeap().adjust(-10, group);
            verifyUsedReclaiming(0, 0);

            allocator.offHeap().adjust(10, group);
            verifyUsedReclaiming(10, 0);

            // confirm we cannot allocate negative
            boolean success = false;
            try
            {
                allocator.offHeap().allocate(-10, group);
            }

            catch (AssertionError e)
            {
                success = true;
            }

            assertTrue(success);
            Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
            assertEquals(1, isClean.getCount());

            // allocate above watermark
            allocator.allocate(70, group);
            verifyUsedReclaiming(80, 0);

            // let the cleaner run, it will release 80 bytes
            canClean.countDown();
            try
            {
                isClean.await(10L, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError();
            }

            assertEquals(0, isClean.getCount());
            verifyUsedReclaiming(0, 0);

            // allocate, then set discarding, then allocated some more
            allocator.allocate(30, group);
            verifyUsedReclaiming(30, 0);
            allocator.setDiscarding();
            assertFalse(allocator.isLive());
            verifyUsedReclaiming(30, 30);
            allocator.allocate(50, group);
            verifyUsedReclaiming(80, 80);

            // allocate above limit, check we block until "marked blocking"
            exec.schedule(markBlocking, 10L, TimeUnit.MILLISECONDS);
            allocator.allocate(30, group);
            assertNotNull(barrier.get());
            verifyUsedReclaiming(110, 110);

            // release everything
            allocator.setDiscarded();
            assertFalse(allocator.isLive());
            verifyUsedReclaiming(0, 0);
        };
        exec.submit(test).get();
    }
}
