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

import java.util.HashSet;
import java.util.Set;

/**
 * A class that extracts system properties for the cassandra node it runs within.
 */
public class SysPropertiesConfig
{
    public static final Set<String> CASSANDRA_RELEVANT_PROPERTIES = new HashSet<>();
    public static final Set<String> CASSANDRA_RELEVANT_ENVS = new HashSet<>();

    public enum CassandraRelevantProperties {
        //base JVM properties
        JAVA_HOME ("java.home",null),
        CASSANDRA_PID_FILE ("cassandra-pidfile", null),
        JAVA_IO_TMPDIR ("java.io.tmpdir", null),
        JAVA_LIBRARY_PATH ("java.library.path", null),
        JAVA_SECURITY_EGD ("java.security.egd", null),
        JAVA_VERSION ("java.version", null),
        JAVA_VM_NAME ("java.vm.name", null),
        LINE_SEPARATOR ("line.separator", null),
        JAVA_CLASS_PATH("java.class.path", null),
        OS_ARCH ("os.arch", null),
        OS_NAME ("os.name", null),
        USER_HOME ("user.home", null),
        SUN_ARCH_DATA_MODEL ("sun.arch.data.model", null),
        //JMX properties
        JAVA_RMI_SERVER_HOSTNAME ("java.rmi.server.hostname", null),
        JAVA_RMI_SERVER_RANDOM_ID ("java.rmi.server.randomID", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE ("com.sun.management.jmxremote.authenticate", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT ("com.sun.management.jmxremote.rmi.port", 0),
        CASS_JMX_REMOTE_PORT ("cassandra.jmx.remote.port", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL ("com.sun.management.jmxremote.ssl", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH ("com.sun.management.jmxremote.ssl.need.client.auth", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE ("com.sun.management.jmxremote.access.file", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE ("com.sun.management.jmxremote.password.file", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_PORT ("com.sun.management.jmxremote.port", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS ("com.sun.management.jmxremote.ssl.enabled.protocols", null),
        COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES ("com.sun.management.jmxremote.ssl.enabled.cipher.suites", null),
        MX4JADDRESS ("mx4jaddress", null),
        MX4JPORT ("mx4jpor", null),
        //cassandra properties (without the "cassandra." prefix)
        CASSANDRA_FOREGROUND ("cassandra-foreground", null),
        DEFAULT_PROVIDE_OVERLAPPING_TOMBSTONES ("default.provide.overlapping.tombstones", null),
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION ("org.apache.cassandra.disable_mbean_registration", null),
        //only for testing
        ORG_APACHE_CASSANDRA_DB_VIRTUAL_SYSTEM_PROPERTIES_TABLE_TEST("org.apache.cassandra.db.virtual.SystemPropertiesTableTest", null);

        CassandraRelevantProperties(String key, Object defaultVal) {
            this.key = key;
            this.defaultVal = defaultVal;
            CASSANDRA_RELEVANT_PROPERTIES.add(key);
        }

        final String key;
        final Object defaultVal;

        String getValue()
        {
            return System.getProperty(key);

        }

        Boolean getBoolean()
        {
            return Boolean.getBoolean(key);
        }

        Integer getInteger()
        {
            return Integer.getInteger(key, (int) defaultVal);
        }

        /** Returns the key used to lookup this system property. */
        public String getKey()
        {
            return key;
        }
    }

    /**
     * Indicates the temporary directory used by the Java Virtual Machine (JVM)
     * to create and store temporary files.
     *
     * @return java.io.tmpdir
     */
    public static String getJavaIoTmpDir()
    {
        return CassandraRelevantProperties.JAVA_IO_TMPDIR.getValue();
    }

    /**
     * Path from which to load native libraries.
     * Default is absolute path to lib directory.
     *
     * @return java.library.path
     */
    public static String getJavaLibraryPath()
    {
        return CassandraRelevantProperties.JAVA_LIBRARY_PATH.getValue();
    }

    public static String getJavaSecurityEgd()
    {
        return CassandraRelevantProperties.JAVA_SECURITY_EGD.getValue();
    }

    /**
     * Java Runtime Environment version
     *
     * @return java.version
     */
    public static String getJavaVersion()
    {
        return CassandraRelevantProperties.JAVA_VERSION.getValue();
    }

    /**
     * @return Java Virtual Machine implementation name
     */
    public static String getJavaVMname()
    {
        return CassandraRelevantProperties.JAVA_VM_NAME.getValue();
    }

    /**
     * @return Sequence used by operating system to separate lines in text files
     */
    public static String getLineSeparator()
    {
        return CassandraRelevantProperties.LINE_SEPARATOR.getValue();
    }

    /**
     * @return Java path
     */
    public static String getJavaClassPath()
    {
        return CassandraRelevantProperties.JAVA_CLASS_PATH.getValue();
    }

    /**
     * @return Accessor for os.arch - Operating system architecture
     */
    public static String getOsArch()
    {
        return CassandraRelevantProperties.OS_ARCH.getValue();
    }

    /**
     * @return Accessor for os.name - Operating system name
     */
    public static String getOsName()
    {
        return CassandraRelevantProperties.OS_NAME.getValue();
    }

    /**
     * @return User's home directory
     */
    public static String getUserHome()
    {
        return CassandraRelevantProperties.USER_HOME.getValue();
    }

    /**
     * @return Returns platform word size sun.arch.data.model. Examples: "32", "64", "unknown"
     */
    public static String getSunArchDataModel() { return CassandraRelevantProperties.SUN_ARCH_DATA_MODEL.getValue(); }

    /**
     * The value of this property represents the host name string
     * that should be associated with remote stubs for locally created remote objects,
     * in order to allow clients to invoke methods on the remote object.
     *
     * @return java.rmi.server.hostname
     */
    public static String getJavaRMIServerHostname() { return CassandraRelevantProperties.JAVA_RMI_SERVER_HOSTNAME.getValue(); }

    /**
     * @return whether password authentication for remote monitoring is
     * enabled. By default it is disabled - com.sun.management.jmxremote.authenticate
     */
     public static boolean isRequiredMgmtJMXRemoteAuth()
     {
         return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_AUTHENTICATE.getBoolean();
     }

    /**
     * @return the port number to which the RMI connector will be bound - com.sun.management.jmxremote.rmi.port.
     * An Integer object that represents the value of the second argument is returned
     * if there is no port specified, if the port does not have the correct numeric format,
     * or if the specified name is empty or null.
     */
     public static int getRMIPort() { return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_RMI_PORT.getInteger(); }

    /**
     * @return Cassandra jmx remote port
     */
     public static String getCassJMXport() { return CassandraRelevantProperties.CASS_JMX_REMOTE_PORT.getValue(); }

    /**
     * @return whether SSL is enabled for monitoring remotely. Default is set to false.
     */
    public static boolean isJMXRemoteMonitoringSSL()
    {
        return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL.getBoolean();
    }

    /**
     * @return whether SSL client authentication is enabled - com.sun.management.jmxremote.ssl.need.client.auth.
     * Default is set to false
     */
    public static boolean isRequiredClientAuth()
    {
        return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_NEED_CLIENT_AUTH.getBoolean();
    }

    /**
     * @return the location for the access file. If com.sun.management.jmxremote.authenticate is false,
     * then this property and the password and access files, are ignored. Otherwise, the access file must exist and
     * be in the valid format. If the access file is empty or nonexistent, then no access is allowed.
     */
    public static String getJMXAccessFile() { return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_ACCESS_FILE.getValue(); }

    /**
     * @return the path to the password file - com.sun.management.jmxremote.password.file
     */
    public static String getJMXPasswordFile() { return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PASSWORD_FILE.getValue(); }

    /**
     * @return port number to enable JMX RMI connections - com.sun.management.jmxremote.port
     */
    public static String getRMIConnectionsPort() { return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PORT.getValue(); }

    /**
     * @return a comma-delimited list of SSL/TLS protocol versions to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.protocols
     */
    public static String getProtocolList()
    {
        return (String) CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_PROTOCOLS.getValue();
    }

    /**
     * @return a comma-delimited list of SSL/TLS cipher suites to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.cipher.suites
     */
    public static String getCipherList()
    {
        return CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_SSL_ENABLED_CIPHER_SUITES.getValue();
    }

    /**
     * @return mx4jaddress
     */
    public static String getMX4JAddress() { return CassandraRelevantProperties.MX4JADDRESS.getValue(); }

    /**
     * @return mx4jport
     */
    public static String getMX4JPort() { return CassandraRelevantProperties.MX4JPORT.getValue(); }

    /**
     * The cassandra-foreground option will tell CassandraDaemon whether
     * to close stdout/stderr, but it's up to us not to background.
     * yes/null
     *
     * @return cassandra-foreground
     */
    public static String getCassandraForeground() { return CassandraRelevantProperties.CASSANDRA_FOREGROUND.getValue(); }

    /**
     * @return cassandra-pidfile
     */
    public static String getCassandraPIDFile()
    {
        return CassandraRelevantProperties.CASSANDRA_PID_FILE.getValue();
    }

    /**
     * @return whether disable_mbean_registration is true
     */
    public static boolean isDisabledMbeanRegistration()
    {
        return Boolean.getBoolean("org.apache.cassandra.disable_mbean_registration");
    }

    public enum CassandraRelevantEnv {
        JAVA_HOME ("JAVA_HOME");

        CassandraRelevantEnv(String key) {
            this.key = key;
            CASSANDRA_RELEVANT_ENVS.add(key);
        }

        final String key;

        String get()
        {
            return System.getenv(key);
        }
    }

    /**
     * Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
     * JVM might use the JRE which do not contains jmap.
     *
     * @return java.home - Java installation directory
     *
     */

    public static String getJavaHome()
    {
        return CassandraRelevantEnv.JAVA_HOME.get();
    }
}
