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

package org.apache.cassandra.config;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * Configuration settings for guardrails populated from the Yaml file.
 *
 * <p>Note that the settings here must only be used to build the {@link GuardrailsConfig} class and not directly by the
 * code checking each guarded constraint. That code should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>This contains a main setting, {@code enabled}, controlling if guardrails are globally active or not, and
 * individual settings to control each guardrail.
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (abort) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 */
public class GuardrailsOptions implements GuardrailsConfig
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsOptions.class);
    
    private final Config config;

    public GuardrailsOptions(Config config)
    {
        this.config = config;
        validateIntThreshold(config.keyspaces_warn_threshold, config.keyspaces_abort_threshold, "keyspaces");
        validateIntThreshold(config.tables_warn_threshold, config.tables_abort_threshold, "tables");
        validateIntThreshold(config.columns_per_table_warn_threshold, config.columns_per_table_abort_threshold, "columns_per_table");
        validateIntThreshold(config.secondary_indexes_per_table_warn_threshold, config.secondary_indexes_per_table_abort_threshold, "secondary_indexes_per_table");
        validateIntThreshold(config.materialized_views_per_table_warn_threshold, config.materialized_views_per_table_abort_threshold, "materialized_views_per_table");
        config.table_properties_ignored = validateTableProperties(config.table_properties_ignored, "table_properties_ignored");
        config.table_properties_disallowed = validateTableProperties(config.table_properties_disallowed, "table_properties_disallowed");
        validateIntThreshold(config.page_size_warn_threshold, config.page_size_abort_threshold, "page_size");
    }

    @Override
    public boolean getEnabled()
    {
        return config.guardrails_enabled;
    }

    /**
     * Enable/disable guardrails.
     *
     * @param enabled {@code true} for enabling guardrails, {@code false} for disabling them.
     */
    public void setEnabled(boolean enabled)
    {
        updatePropertyWithLogging("guardrails_enabled",
                                  enabled,
                                  () -> config.guardrails_enabled,
                                  x -> config.guardrails_enabled = x);
    }

    @Override
    public int getKeyspacesWarnThreshold()
    {
        return config.keyspaces_warn_threshold;
    }

    @Override
    public int getKeyspacesAbortThreshold()
    {
        return config.keyspaces_abort_threshold;
    }

    public void setKeyspacesThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "keyspaces");
        updatePropertyWithLogging("keyspaces_warn_threshold",
                                  warn,
                                  () -> config.keyspaces_warn_threshold,
                                  x -> config.keyspaces_warn_threshold = x);
        updatePropertyWithLogging("keyspaces_abort_threshold",
                                  abort,
                                  () -> config.keyspaces_abort_threshold,
                                  x -> config.keyspaces_abort_threshold = x);
    }

    @Override
    public int getTablesWarnThreshold()
    {
        return config.tables_warn_threshold;
    }

    @Override
    public int getTablesAbortThreshold()
    {
        return config.tables_abort_threshold;
    }

    public void setTablesThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "tables");
        updatePropertyWithLogging("tables_warn_threshold",
                                  warn,
                                  () -> config.tables_warn_threshold,
                                  x -> config.tables_warn_threshold = x);
        updatePropertyWithLogging("tables_abort_threshold",
                                  abort,
                                  () -> config.tables_abort_threshold,
                                  x -> config.tables_abort_threshold = x);
    }

    @Override
    public int getColumnsPerTableWarnThreshold()
    {
        return config.columns_per_table_warn_threshold;
    }

    @Override
    public int getColumnsPerTableAbortThreshold()
    {
        return config.columns_per_table_abort_threshold;
    }

    public void setColumnsPerTableThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "columns_per_table");
        updatePropertyWithLogging("columns_per_table_warn_threshold",
                                  warn,
                                  () -> config.columns_per_table_warn_threshold,
                                  x -> config.columns_per_table_warn_threshold = x);
        updatePropertyWithLogging("columns_per_table_abort_threshold",
                                  abort,
                                  () -> config.columns_per_table_abort_threshold,
                                  x -> config.columns_per_table_abort_threshold = x);
    }

    @Override
    public int getSecondaryIndexesPerTableWarnThreshold()
    {
        return config.secondary_indexes_per_table_warn_threshold;
    }

    @Override
    public int getSecondaryIndexesPerTableAbortThreshold()
    {
        return config.secondary_indexes_per_table_abort_threshold;
    }

    public void setSecondaryIndexesPerTableThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "secondary_indexes_per_table");
        updatePropertyWithLogging("secondary_indexes_per_table_warn_threshold",
                                  warn,
                                  () -> config.secondary_indexes_per_table_warn_threshold,
                                  x -> config.secondary_indexes_per_table_warn_threshold = x);
        updatePropertyWithLogging("secondary_indexes_per_table_abort_threshold",
                                  abort,
                                  () -> config.secondary_indexes_per_table_abort_threshold,
                                  x -> config.secondary_indexes_per_table_abort_threshold = x);
    }

    @Override
    public int getMaterializedViewsPerTableWarnThreshold()
    {
        return config.materialized_views_per_table_warn_threshold;
    }

    @Override
    public int getMaterializedViewsPerTableAbortThreshold()
    {
        return config.materialized_views_per_table_abort_threshold;
    }

    public void setMaterializedViewsPerTableThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "materialized_views_per_table");
        updatePropertyWithLogging("materialized_views_per_table_warn_threshold",
                                  warn,
                                  () -> config.materialized_views_per_table_warn_threshold,
                                  x -> config.materialized_views_per_table_warn_threshold = x);
        updatePropertyWithLogging("materialized_views_per_table_abort_threshold",
                                  abort,
                                  () -> config.materialized_views_per_table_abort_threshold,
                                  x -> config.materialized_views_per_table_abort_threshold = x);
    }

    @Override
    public int getPageSizeWarnThreshold()
    {
        return config.page_size_warn_threshold;
    }

    @Override
    public int getPageSizeAbortThreshold()
    {
        return config.page_size_abort_threshold;
    }

    public void setPageSizeThreshold(int warn, int abort)
    {
        validateIntThreshold(warn, abort, "page_size");
        updatePropertyWithLogging("page_size_warn_threshold",
                                  warn,
                                  () -> config.page_size_warn_threshold,
                                  x -> config.page_size_warn_threshold = x);
        updatePropertyWithLogging("page_size_abort_threshold",
                                  abort,
                                  () -> config.page_size_abort_threshold,
                                  x -> config.page_size_abort_threshold = x);
    }

    @Override
    public Set<String> getTablePropertiesIgnored()
    {
        return config.table_properties_ignored;
    }

    public void setTablePropertiesIgnored(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_ignored",
                                  validateTableProperties(properties, "table_properties_ignored"),
                                  () -> config.table_properties_ignored,
                                  x -> config.table_properties_ignored = x);
    }

    @Override
    public Set<String> getTablePropertiesDisallowed()
    {
        return config.table_properties_disallowed;
    }

    public void setTablePropertiesDisallowed(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_disallowed",
                                  validateTableProperties(properties, "table_properties_disallowed"),
                                  () -> config.table_properties_disallowed,
                                  x -> config.table_properties_disallowed = x);
    }

    @Override
    public boolean getUserTimestampsEnabled()
    {
        return config.user_timestamps_enabled;
    }

    public void setUserTimestampsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("user_timestamps_enabled",
                                  enabled,
                                  () -> config.user_timestamps_enabled,
                                  x -> config.user_timestamps_enabled = x);
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return config.read_before_write_list_operations_enabled;
    }

    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("read_before_write_list_operations_enabled",
                                  enabled,
                                  () -> config.read_before_write_list_operations_enabled,
                                  x -> config.read_before_write_list_operations_enabled = x);
    }

    private static <T> void updatePropertyWithLogging(String propertyName, T newValue, Supplier<T> getter, Consumer<T> setter)
    {
        T oldValue = getter.get();
        if (!newValue.equals(oldValue))
        {
            setter.accept(newValue);
            logger.info("Updated {} from {} to {}", propertyName, oldValue, newValue);
        }
    }

    private static void validatePositiveNumeric(long value, long maxValue, boolean allowZero, String name)
    {
        if (value > maxValue)
            throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                      value, name, maxValue));

        if (value == 0 && !allowZero)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use %d",
                                                      name, Config.DISABLED_GUARDRAIL));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value < Config.DISABLED_GUARDRAIL)
            throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                      "outside of %d which disables the guardrail",
                                                      value, name, Config.DISABLED_GUARDRAIL));
    }

    private static void validateStrictlyPositiveInteger(long value, String name)
    {
        // We use 'long' for generality, but most numeric guardrail cannot effectively be more than a 'int' for various
        // internal reasons. Not that any should ever come close in practice ...
        // Also, in most cases, zero does not make sense (allowing 0 tables or columns is not exactly useful).
        validatePositiveNumeric(value, Integer.MAX_VALUE, false, name);
    }

    private static void validateIntThreshold(int warn, int abort, String name)
    {
        validateStrictlyPositiveInteger(warn, name + "_warn_threshold");
        validateStrictlyPositiveInteger(abort, name + "_abort_threshold");
        validateWarnLowerThanAbort(warn, abort, name);
    }

    private static void validateWarnLowerThanAbort(long warn, long abort, String name)
    {
        if (warn == Config.DISABLED_GUARDRAIL || abort == Config.DISABLED_GUARDRAIL)
            return;

        if (abort < warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be lower " +
                                                      "than the abort threshold %d", warn, name, abort));
    }

    private static Set<String> validateTableProperties(Set<String> properties, String name)
    {
        if (properties == null)
            throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

        Set<String> lowerCaseProperties = properties.stream().map(String::toLowerCase).collect(toSet());

        Set<String> diff = Sets.difference(lowerCaseProperties, TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new IllegalArgumentException(format("Invalid value for %s: '%s' do not parse as valid table properties",
                                                      name, diff));

        return lowerCaseProperties;
    }
}
