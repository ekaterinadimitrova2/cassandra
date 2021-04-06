# License Compliance

The target of this document is to provide an overview and guidance how the Apache Cassandra project's source code and
artifacts maintain compliance with the [ASF Licensing policy](http://www.apache.org/legal/release-policy.html#licensing). 

The repository contains a LICENSE file, and a NOTICE file.

The Apache Cassandra project enforces and verifies ASF License header conformance on all source files using the Apache RAT tool.

With a few exceptions, source files consisting of works submitted directly to the ASF by the copyright owner or owner's
agent must contain the appropriate ASF license header. Files without any degree of creativity don't require a license header.

Currently, RAT checks all  *.java, *.py, *sh, and *xml files for a LICENSE header.

If there is an incompliance, the build will fail with the following warning:
```java 
Some files have missing or incorrect license information. Check RAT report in ${build.dir}/src.rat.txt for more details!
```