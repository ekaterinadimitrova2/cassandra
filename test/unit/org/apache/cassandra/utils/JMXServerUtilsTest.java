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

package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JMXServerUtilsTest
{
    private static final String CERTIFICATE_ALIAS = "UNIT_TEST_SELF_SIGNED";
    private static final String PASSWORD = "super_secret";

    private JMXConnectorServer jmxServer;

    static
    {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File keystoreFile;
    private KeyPair keyPair;

    @After
    public void teardown()
    {
        if (jmxServer != null)
        {
            try
            {
                jmxServer.stop();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                jmxServer = null;
            }
        }

        keyPair = null;
        keystoreFile = null;
    }

    @Before
    public void clearSystemProperties()
    {
        for (String prop : new String[]{
        "cassandra.jmx.authorizer",
        "cassandra.jmx.local.port",
        "cassandra.jmx.remote.port",
        "cassandra.jmx.remote.login.config",
        "java.rmi.server.hostname",
        "java.security.auth.login.config",
        "javax.net.ssl.keyStore",
        "javax.net.ssl.keyStorePassword",
        "javax.net.ssl.trustStore",
        "javax.net.ssl.trustStorePassword",
        "com.sun.management.jmxremote.authenticate",
        "com.sun.management.jmxremote.host",
        "com.sun.management.jmxremote.rmi.port",
        "com.sun.management.jmxremote.ssl",
        "com.sun.management.jmxremote.ssl.need.client.auth",
        "com.sun.management.jmxremote.ssl.enabled.protocols",
        "com.sun.management.jmxremote.ssl.enabled.cipher.suites",
        "com.sun.management.jmxremote.password.file",
        "com.sun.management.jmxremote.access.file"
        })
        {
            System.getProperties().remove(prop);
            assertNull(System.getProperty(prop));
        }
    }

    @Test
    public void jmxServerLocal() throws IOException
    {
        System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());

        int port = randomTcpPort();

        jmxServer = JMXServerUtils.createJMXServer(port, true);
        assertTrue("must be bound to 127.0.0.1", validateBound(port, InetAddress.getLoopbackAddress().getHostAddress()));
        assertFalse("must not be bound to 127.123.123.2", validateBound(port, "127.123.123.2"));

        verifyJMX(false, InetAddress.getLoopbackAddress().getHostAddress(), port);
    }

    @Test
    public void jmxServerBindToAny() throws IOException
    {
        System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());

        int port = randomTcpPort();

        jmxServer = JMXServerUtils.createJMXServer(port, false);
        assertTrue("must be bound to <any>", validateBound(port, null));
        assertFalse("must not be bound to 127.0.0.1", validateBound(port, "127.0.0.1"));
        assertFalse("must not be bound to 127.0.0.2", validateBound(port, "127.0.0.2"));

        verifyJMX(false, "127.1.2.3", port);
    }

    @Test
    public void jmxServerBindToSpecific() throws IOException
    {
        System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());

        int port = randomTcpPort();
        System.setProperty("com.sun.management.jmxremote.host", "127.0.0.2");

        jmxServer = JMXServerUtils.createJMXServer(port, false);
        assertFalse("must not be bound to 127.0.0.1", validateBound(port, "127.0.0.1"));
        assertTrue("must be bound to 127.0.0.2", validateBound(port, "127.0.0.2"));

        verifyJMX(false, "127.0.0.2", port);
    }

    @Test
    public void sslJmxServerBindToAny() throws IOException
    {
        System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());

        int port = randomTcpPort();
        System.setProperty("com.sun.management.jmxremote.host", "127.0.0.2");

        prepSSL();

        jmxServer = JMXServerUtils.createJMXServer(port, true);
        assertTrue("must be bound to <any>", validateBound(port, null));
        assertFalse("must not be bound to 127.0.0.1", validateBound(port, "127.0.0.1"));
        assertFalse("must not be bound to 127.0.0.2", validateBound(port, "127.0.0.2"));

        verifyJMX(true, "127.1.2.3", port);
    }

    private void verifyJMX(boolean ssl, String host, int port)
    {
        try
        {
            clearSystemProperties();

            RMIClientSocketFactory clientFactory = ssl ? new SslRMIClientSocketFactory() : RMISocketFactory.getDefaultSocketFactory();

            Map<String, Object> env = new HashMap<>();
            env.put("com.sun.jndi.rmi.factory.socket", clientFactory);

            JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi", host, port));
            try (JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, env))
            {
                MBeanServerConnection mbeanServerConn = jmxc.getMBeanServerConnection();
                RuntimeMXBean proxy = JMX.newMBeanProxy(mbeanServerConn, new ObjectName(ManagementFactory.RUNTIME_MXBEAN_NAME), RuntimeMXBean.class);
                assertNotNull(proxy.getName());
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void prepSSL()
    {
        createKeyTrustStores();

        System.setProperty("com.sun.management.jmxremote.ssl", "true");
        System.setProperty("com.sun.management.jmxremote.ssl.need.client.auth", "false");
        System.setProperty("javax.net.ssl.keyStore", keystoreFile.getAbsolutePath());
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStore", keystoreFile.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
    }

    private void createKeyTrustStores()
    {
        try
        {
            KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
            keyPairGenerator.initialize(512, new SecureRandom());
            keyPair = keyPairGenerator.generateKeyPair();
            SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

            X500Name issuer = new X500Name("CN=PandE, O=DataStax, L=foo, ST=CA, C=US");
            BigInteger serialNumber = BigInteger.valueOf(123);
            Date notBefore = new Date(System.currentTimeMillis() - 1000L * 60 * 60 * 24);
            Date notAfter = new Date(System.currentTimeMillis() + (1000L * 60 * 60 * 24 * 365 * 10));
            X500Name subject = new X500Name("CN=PandE, O=DataStax, L=foo, ST=CA, C=US");

            X509v3CertificateBuilder builder = new X509v3CertificateBuilder(issuer,
                                                                            serialNumber,
                                                                            notBefore,
                                                                            notAfter,
                                                                            subject,
                                                                            subPubKeyInfo);
            ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSAEncryption")
                                   .build(keyPair.getPrivate());
            X509CertificateHolder certHolder = builder.build(signer);
            X509Certificate cert = new JcaX509CertificateConverter()
                                   .setProvider(new BouncyCastleProvider())
                                   .getCertificate(certHolder);
            cert.verify(keyPair.getPublic());

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

            ks.load(null, PASSWORD.toCharArray());
            ks.setKeyEntry(CERTIFICATE_ALIAS, keyPair.getPrivate(), PASSWORD.toCharArray(), new java.security.cert.Certificate[]{ cert });

            keystoreFile = temporaryFolder.newFile();

            // Store away the keystore.
            try (FileOutputStream fos = new FileOutputStream(keystoreFile))
            {
                ks.store(fos, PASSWORD.toCharArray());
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create SSL stuff", e);
        }
    }

    private static int randomTcpPort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            return serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static boolean validateBound(int port, String boundTo)
    {
        Process proc = null;
        try
        {
            proc = new ProcessBuilder(Arrays.asList("lsof",
                                                    "-nP",
                                                    "-iTCP" + (boundTo != null ? '@' + boundTo : "") + ':' + port)).start();
            if (!proc.waitFor(5, TimeUnit.SECONDS))
                fail("lsof did not terminate within 5 seconds (that is too long...)");

            return proc.exitValue() == 0;
        }
        catch (Exception e)
        {
            if (proc != null)
                proc.destroyForcibly();
            throw new RuntimeException(e);
        }
    }
}
