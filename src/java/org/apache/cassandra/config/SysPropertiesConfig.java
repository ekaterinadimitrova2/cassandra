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

import com.google.common.collect.Sets;

/**
 * A class that extracts system properties for the cassandra node it runs within.
 */

public class SysPropertiesConfig
{
    public static final Set<String> CASSANDRA_RELEVANT_PROPERTIES = Sets.newHashSet(
        // base jvm properties
        "java.home",
        "java.io.tmpdir",
        "java.library.path",
        "java.security.egd",
        "java.version",
        "java.vm.name",
        "line.separator",
        "os.arch",
        "os.name",
        "user.home",
        "sun.arch.data.model",
        // jmx properties
        "java.rmi.server.hostname",
        "java.rmi.server.randomID",
        "com.sun.management.jmxremote.authenticate",
        "com.sun.management.jmxremote.rmi.port",
        "com.sun.management.jmxremote.ssl",
        "com.sun.management.jmxremote.ssl.need.client.auth",
        "com.sun.management.jmxremote.access.file",
        "com.sun.management.jmxremote.password.file",
        "com.sun.management.jmxremote.port",
        "com.sun.management.jmxremote.ssl.enabled.protocols",
        "com.sun.management.jmxremote.ssl.enabled.cipher.suites",
        "mx4jaddress",
        "mx4jport",
        // cassandra properties (without the "cassandra." prefix)
        "cassandra-foreground",
        "cassandra-pidfile",
        "default.provide.overlapping.tombstones",
        "org.apache.cassandra.disable_mbean_registration",
        // only for testing
        "org.apache.cassandra.db.virtual.SystemPropertiesTableTest"
    );

    public static final Set<String> CASSANDRA_RELEVANT_ENVS = Sets.newHashSet(
        "JAVA_HOME"
    );
    /**
     * @return java.home - Java installation directory
     *
     * Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
     * JVM might use the JRE which do not contains jmap.
     */
    public static String getJavaHome() { return System.getenv("JAVA_HOME"); }

    /**
     * @return java.io.tmpdir
     * Indicates the temporary directory used by the Java Virtual Machine (JVM)
     * to create and store temporary files.
     */
    public static String getJavaIoTmpDir() { return System.getProperty("java.io.tmpdir"); }

    /**
     * @return java.library.path
     * Path from which to load native libraries.
     * Default is absolute path to lib directory.
     */
    public static String getJavaLibraryPath()
    {
        return System.getProperty("java.library.path");
    }

    public static String getJavaSecurityEgd()
    {
        return System.getProperty("java.security.egd");
    }

    /**
     * @return java.version - Java Runtime Environment version
     */
    public static String getJavaVersion()
    {
        return System.getProperty("java.version");
    }

    /**
     * @return Java Virtual Machine implementation name
     */
    public static String getJavaVMname()
    {
        return System.getProperty("java.vm.name");
    }

    /**
     * @return Sequence used by operating system to separate lines in text files
     */
    public static String getLineSeparator()
    {
        return System.getProperty("line.separator");
    }

    /**
     * @return Java path
     */
    public static String getJavaClassPath()
    {
        return System.getProperty("java.class.path");
    }


    /**
     * @return Accessor for os.arch - Operating system architecture
     */
    public static String getOsArch()
    {
        return System.getProperty("os.arch");
    }

    /**
     * @return Accessor for os.name - Operating system name
     */
    public static String getOsName()
    {
        return System.getProperty("os.name");
    }

    /**
     * @return User's home directory
     */
    public static String getUserHome()
    {
        return System.getProperty("user.home");
    }

    /**
     * @return Returns platform word size
     * sun.arch.data.model. Examples: "32", "64", "unknown"
     */
    public static String getSunArchDataModel() { return System.getProperty("sun.arch.data.model"); }

    /**
     * @return java.rmi.server.hostname
     * The value of this property represents the host name string
     * that should be associated with remote stubs for locally created remote objects,
     * in order to allow clients to invoke methods on the remote object.
     */
    public static String getJavaRMIServerHostname() { return System.getProperty("java.rmi.server.hostname"); }

    /**
     * @return whether password authentication for remote monitoring is
     * enabled. By default it is disabled - com.sun.management.jmxremote.authenticate
     */
     public static boolean isRequiredMgmtJMXRemoteAuth()
     {
         return Boolean.getBoolean("com.sun.management.jmxremote.authenticate");
     }

    /**
     * @return the port number to which the RMI connector will be bound - com.sun.management.jmxremote.rmi.port.
     * An Integer object that represents the value of the second argument is returned
     * if there is no port specified, if the port does not have the correct numeric format,
     * or if the specified name is empty or null.
     */
     public static int getRMiPort() { return Integer.getInteger("com.sun.management.jmxremote.rmi.port", 0); }

    /**
     * @return Cassandra jmx remote port
     */
     public static String getCassJMXport() {return System.getProperty("cassandra.jmx.remote.port"); }

    /**
     * @return whether SSL is enabled for monitoring remotely. Default is set to false.
     */
    public static boolean isJMXRemoteMonitoringSSL()
    {
        return Boolean.getBoolean("com.sun.management.jmxremote.ssl");
    }

    /**
     * @return whether SSL client authentication is enabled - com.sun.management.jmxremote.ssl.need.client.auth.
     * Default is set to faulse
     */
    public static boolean isRequiredClientAuth()
    {
        return Boolean.getBoolean("com.sun.management.jmxremote.ssl.need.client.auth");
    }

    /**
     * @return the location for the access file. If com.sun.management.jmxremote.authenticate is false,
     * then this property and the password and access files, are ignored. Otherwise, the access file must exist and
     * be in the valid format. If the access file is empty or nonexistent, then no access is allowed.
     */
    public static String getAccessFile() { return System.getProperty("com.sun.management.jmxremote.access.file"); }

    /**
     * @return the path to the password file - com.sun.management.jmxremote.password.file
     */
    public static String getPasswordFile() { return System.getProperty("com.sun.management.jmxremote.password.file"); }

    /**
     * @return port number to enable JMX RMI connections - com.sun.management.jmxremote.port
     */
    public static String getRMIConnectionsPort() { return System.getProperty("com.sun.management.jmxremote.port"); }

    /**
     * @return a comma-delimited list of SSL/TLS protocol versions to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.protocols
     */
    public static String getProtocolList()
    {
        return System.getProperty("com.sun.management.jmxremote.ssl.enabled.protocols");
    }

    /**
     * @return a comma-delimited list of SSL/TLS cipher suites to enable.
     * Used in conjunction with com.sun.management.jmxremote.ssl - com.sun.management.jmxremote.ssl.enabled.cipher.suites
     */
    public static String getCipherList()
    {
        return System.getProperty("com.sun.management.jmxremote.ssl.enabled.cipher.suites");
    }

    /**
     * @return mx4jaddress
     */
    public static String getSAddress() { return System.getProperty("mx4jaddress"); }

    /**
     * @return mx4jport
     */
    public static String getSPort() { return System.getProperty("mx4jport"); }

    /**
     * @return cassandra-foreground
     * The cassandra-foreground option will tell CassandraDaemon whether
     * to close stdout/stderr, but it's up to us not to background.
     * yes/null
     */
    public static String getCassandraForeground() { return System.getProperty("cassandra-foreground"); }

    /**
     * @return cassandra-pidfile
     */
    public static String getCassandraPIDFile() { return System.getProperty("cassandra-pidfile"); }

    /**
     * @return whether disable_mbean_registration is true
     */
    public static boolean isDisabledMbeanRegistration()
    {
        return Boolean.getBoolean("org.apache.cassandra.disable_mbean_registration");
    }

}
