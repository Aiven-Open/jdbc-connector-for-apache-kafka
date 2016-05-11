Kafka Connect JDBC Sink
=======================

Kafka Connect JDBC Sink is a connector to write data from Kafka to a
sink target that supports JDBC.

.. toctree::
   :maxdepth: 3

Prerequisites
-------------

-  Confluent 2.0.1
-  Java 1.7

Setup
-----

Sink Connector QuickStart
-------------------------

Test Data
~~~~~~~~~

Deploying the Sink Connector
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sink Connector Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

... code:: bash

Starting the Sink Connector (Distributed)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A start in distributed mode.

.. code:: bash

    ➜  bin/connect-distributed \
    ➜  etc/schema-registry/connect-avro-distributed.properties 

Once the connector has started lets use the kafka-connect-tools cli to
post in our distributed properties file.

.. code:: bash

    ➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar \
    ➜  create jdbc-sink < jdbc-sink-distributed-orders.properties 

Now check the logs to see if we started the sink.

... code:: bash

Now check the database

... code:: bash

Features
--------

Error Polices
~~~~~~~~~~~~~

Insert Mode
~~~~~~~~~~~

Update Mode
~~~~~~~~~~~

Field Selection
~~~~~~~~~~~~~~~

Topic Routing
~~~~~~~~~~~~~

Schema Evolution
~~~~~~~~~~~~~~~~

Configuration
-------------

The JDBC connector gives you quite a bit of flexibility in the databases you can export data to and how that data is exported. This section first
describes how to access databases whose drivers are not included with Confluent Platform, then gives a few example configuration files thatcover common scenarios, then provides an exhaustive description of the available configuration options.

JDBC Drivers
~~~~~~~~~~~~

The JDBC connector implements the data copying functionality on the
generic JDBC APIs, but relies on JDBC drivers to handle the
database-specific implementation of those APIs. Confluent Platform ships
with a few JDBC drivers, but if the driver for your database is not
included you will need to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector.
The packaged connector is installed in the
``share/java/kafka-connect-jdbc`` directory, relative to the
installation directory. If you have installed from Debian or RPM
packages, the connector will be installed in
``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or tar
files, the connector will be installed in the path given above under the
directory where you unzipped the Confluent Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running For
example:

.. code:: bash

    $ CLASSPATH=/usr/local/firebird/* ./bin/copycat-distributed ./config/copycat-distributed.properties

would add the JDBC driver for the Firebird database, located in
``/usr/local/firebird``, and allow you to use JDBC connection URLs like
``jdbc:firebirdsql:localhost/3050:/var/lib/firebird/example.db``.

JDBC Sink Connector Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``connect.jdbc.connection.uri``

* Type: string
* Importance: high

Specifies the JDBC database connection URI.

``connect.jdbc.connection.user``

* Type: string
* Importance: high
  
Specifies the JDBC connection user

``connect.jdbc.connection.password``

* Type: password (shows ``[hidden]``) in logs
* Importance: high

Specifies the JDBC connection password.

``connect.jdbc.sink.batching.enabled``

* Type: boolean
* Importance: high

Specifies if a given sequence of SinkRecords are batched or not. If ``true`` the data insert is batched else for each record a sql statement is created.";

``connect.jdbc.sink.driver.jar``
  
* Type: string
* Importance: high

Specifies the jar file to be loaded at runtime containing the jdbc driver.

``connect.jdbc.sink.driver.manager.class``

* Type: string
* Importance: high
 
Specifies the canonical class name for the driver manager.

``connect.jdbc.sink.error.policy``

* Type: string
* Importance: high
* Default: ``throw``
  
Specifies the action to be taken if an error occurs while inserting the data.There are two available options: ``noop`` - the error is swallowed ``throw`` - the error is allowed to propagate. The error will be logged automatically

``connect.jdbc.sink.mode``
 
* Type: string
* Importance: high

Specifies how the data should be landed into the RDBMS. Two options are supported: ``INSERT`` (default value) and ``UPSERT``

 ``connect.jdbc.sink.topics.to.tables``

* Type: string
* Importance: high

Specifies which topic maps to which table.Example:topic1=table1;topic2=table2.

``connect.jdbc.sink.table.[table].mappings``

* Type: string
* Importance: high

Specifies which fields and there mapping to table columns should be extracted from the SinkRecords.

Examples:

Extract only field1 and field2 from topic A and field3 from topic B

.. code:: bash

    connect.jdbc.sink.table.topicA.mappings=field1,field2
    connect.jdbc.sink.table.topicB.mappings=field3

Extract only field1 and field2 from topic A and field3 from topic B but with alias mapping to different columns. Field 1 from topic A goes to colZ and field 3 from topic B goes to column Y. The topic to table mapping is controlled by ``connect.jdbc.sink.topics.to.tables``

.. code:: bash

    connect.jdbc.sink.table.topicA.mappings=field1=colZ,field2
    connect.jdbc.sink.table.topicB.mappings=field3=colY

Example Configurations
~~~~~~~~~~~~~~~~~~~~~~

Schema Evolution
----------------

TODO

Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
