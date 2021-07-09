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
package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.utils.FBUtilities;

public class ClientWarn implements ExecutorLocal<ClientWarn.State>
{
    private static final String TRUNCATED = " [truncated]";
    private static final FastThreadLocal<State> warnLocal = new FastThreadLocal<>();
    public static ClientWarn instance = new ClientWarn();

    private ClientWarn()
    {
    }

    public State get()
    {
        return warnLocal.get();
    }

    public void set(State value)
    {
        warnLocal.set(value);
    }

    public void warn(String text)
    {
        State state = warnLocal.get();
        if (state != null)
            state.add(text);
    }

    public void captureWarnings()
    {
        warnLocal.set(new State());
    }

    public List<String> getAndClearWarnings()
    {
        State state = warnLocal.get();
        if (state == null)
            return null;

        ArrayList<String> tmp = new ArrayList<>(state.warnings.size());
        state.warnings.drainTo(tmp);
        if (tmp.isEmpty())
            return null;

        return tmp;
    }

    public int numWarnings()
    {
        return getAndClearWarnings() == null ? 0 : getAndClearWarnings().size();
    }

    public void resetWarnings()
    {
        warnLocal.remove();
    }

    public static class State
    {
        private final BlockingQueue<String> warnings = new LinkedBlockingQueue<>();

        private void add(String warning)
        {
            if (warnings.size() < FBUtilities.MAX_UNSIGNED_SHORT)
                warnings.add(maybeTruncate(warning));
        }

        private static String maybeTruncate(String warning)
        {
            return warning.length() > FBUtilities.MAX_UNSIGNED_SHORT
                   ? warning.substring(0, FBUtilities.MAX_UNSIGNED_SHORT - TRUNCATED.length()) + TRUNCATED
                   : warning;
        }

    }
}
