Kafka Connect JDBC Sink
=======================

Kafka Connect JDBC Sink is a connector to write data from Kafka to a sink target that supports JDBC.

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

.. sourcecode:: bash

    ➜  bin/connect-distributed \
    ➜  etc/schema-registry/connect-avro-distributed.properties 

Once the connector has started lets use the kafka-connect-tools cli to post in our distributed properties file.

.. sourcecode:: bash

    ➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar \
    ➜  create jdbc-sink < jdbc-sink-distributed-orders.properties 

Now check the logs to see if we started the sink.

... sourcecode:: bash

Now check the database

... sourcecode:: bash

Features
--------

1. Error Polices.
2. Write modes.
3. Topic to table mappings.
4. Field Selection.
5. Auto create tables.
6. Auto evolve tables.

Error Polices
~~~~~~~~~~~~~

The sink has three error policies that determine how failed writes to the target database are handled.

**Throw**.

Any error on write to the target database will be propagated up and processing stopped. This is the default
behaviour.

**Noop**.

Any error on write to the target database is ignore and processing continues.

.. warning::

    This can lead to missed errors if you don't have adequate monitoring. Data is not lost as it's still in Kafka
    subject to Kafka's retention policy.

**Retry**.

Any error on write to the target database causes the RetryIterable exception to be thrown. This causes the
Kafka connect framework to pause and replay the message. Offsets are not committed. For example, if the table is offline,
by accident or accidentally modified or even having a DDL statement applied cause a write failure, the message can be
replayed.

The error policies effect the behaviour of the schema evolution characteristics of the sink. See the schema evolution
section for more information.

Write Modes
~~~~~~~~~~~

The sink supports both **insert** and **upsert** modes.

**Insert**

In this mode the sink prepares insert statements to execute either in batch transactions or individually dependent on
the ``connect.jdbc.sink.batching.enabled`` setting. Typically you would use this in append only tables such as ledgers. 
Combined with the error policy setting, ``connect.jdbc.sink.error.policy``, this allows for idempotent writes. For
example, sent to NOOP, violations of primary keys would be rejected by the database and sink would log the error but
continue processing but you miss real errors.

**Update**

In this mode the sink prepares upsert statements, the exactly syntax is dependent on the target database.
The SQL dialect is obtained from the connection URI. When the sink tries to write it executes the appropriate upsert
statement, for example with MySQL it will use the
`ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_ to apply an update if a primary key
constraint is violated. If the update fails the sink fails back to the error policy.

The following dialects and upsert statements are supported:

1.  MySQL - `ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_
2.  Oracle - `MERGE <https://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_9016.htm>`_. 
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queries 
    to retrieve this.
3.  MSSQL - `MERGE <https://msdn.microsoft.com/en-us/library/bb510625.aspx>`_.
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queries 
    to retrieve this.
4.  Postgre - 9.5 and above.???? Needs to be implemented.


Topic Routing
~~~~~~~~~~~~~

The sink supports topic routing that allows mapping the messages from topics to a specific table. For example map
a topic called "bloomberg_prices" to a table called "prices". This mapping is set in the
``connect.jdbc.sink.export.mappings`` option.

.. tip::

    Explicit mapping of topics to tables is required. If not present the sink will not start and fail validation checks.

Field Selection
~~~~~~~~~~~~~~~

The sink supports selecting fields from the source topic or selecting all fields and mapping of this fields to columns
in the target table. For example map a field in the topic called "qty" to a column called "quantity" in the target
table.

All fields can be selected by using "*" in the field part of ``connect.jdbc.sink.export.mappings``.

Leaving the column name empty means trying to map to a column in the target table with the same name as the field in the
source topic.

.. tip::

    Check your mappings!

    All database column names are checked at startup if mappings are provided. If they do not exist in the target table the
    configuration will not pass validation checks and refuse to start. If ``connect.jdbc.sink.auto.create`` is enabled
    this does not apply since the table will be created based on the first message received for the topic.

    If no mappings are supplied the checks happen when the first message is received and processed for the source topics.


.. warning::

    Field selection disables evolving the target table if the upstream schema in the Kafka topic changes. By specifying
    field mappings it is assumed the user is not interested in upstream changes. For example they maybe tapping into a
    pipeline for a Kafka stream job and not be intended as the final recipient of the stream.

    If a upstream field is removed and the topic is not following the Schema Registries evolution rules, .i.e. not
    full or backwards compatible, any errors will default to the error policy. If schema evolution rules have been followed
    the missing field will return the default value set in the schema. A warning will be logged if the schema version
    changes.

Auto Create Tables
~~~~~~~~~~~~~~~~~~

TODO

Auto Evolve Tables
~~~~~~~~~~~~~~~~~~

TODO

Configuration
-------------

The JDBC connector gives you quite a bit of flexibility in the databases you can export data to and how that data is
exported. This section first describes how to access databases whose drivers are not included with Confluent Platform,
then gives a few example configuration files that cover common scenarios, then provides an exhaustive description of the
available configuration options.

JDBC Drivers
~~~~~~~~~~~~

The JDBC connector implements the data copying functionality on the generic JDBC APIs, but relies on JDBC drivers to
handle the database-specific implementation of those APIs. Confluent Platform ships with a few JDBC drivers, but if the
driver for your database is not included you will need to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector. The packaged connector is installed in the
``share/java/kafka-connect-jdbc`` directory, relative to the installation directory. If you have installed from Debian
or RPM packages, the connector will be installed in ``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or
tar files, the connector will be installed in the path given above under the directory where you unzipped the Confluent
Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running For
example:

.. sourcecode:: bash

    $ CLASSPATH=/usr/local/firebird/* ./bin/copycat-distributed ./config/copycat-distributed.properties

would add the JDBC driver for the Firebird database, located in ``/usr/local/firebird``, and allow you to use JDBC
connection URLs like ``jdbc:firebirdsql:localhost/3050:/var/lib/firebird/example.db``.

JDBC Sink Connector Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``connect.jdbc.connection.uri``

Specifies the JDBC database connection URI.

* Type: string
* Importance: high

``connect.jdbc.connection.user``

Specifies the JDBC connection user.

* Type: string
* Importance: high

``connect.jdbc.connection.password``

Specifies the JDBC connection password.

* Type: password (shows ``[hidden]``) in logs
* Importance: high

``connect.jdbc.sink.batching.enabled``

Specifies if a given sequence of SinkRecords are batched in a transaction or not. If ``true`` the records delivered to
the task in each `put` is batched in one transaction. Otherwise for each record a inserted in its own transaction.

* Type: boolean
* Importance: high
* Default : true

``connect.jdbc.sink.driver.jar``

Specifies the jar file to be loaded at runtime containing the jdbc driver.
  
* Type: string
* Importance: high

``connect.jdbc.sink.driver.manager.class``

Specifies the canonical class name for the driver manager.

* Type: string
* Importance: high

``connect.jdbc.sink.error.policy``

Specifies the action to be taken if an error occurs while inserting the data.

There are three available options, **noop**, the error is swallowed, **throw**, the error is allowed to propagate and retry. 
For **retry** the Kafka message is redelivered up to a maximum number of times specified by the
``connect.jdbc.sink.max.retries`` option.

The errors will be logged automatically.

* Type: string
* Importance: high
* Default: ``throw``

``connect.jdbc.sink.max.retries``

The maximum number of a message is retried. Only valid when the ``connect.jdbc.sink.error.policy`` is set to ``retry``.

* Type: string
* Importance: high
* Default: 10

The maximum number of a message is retried. Only valid when the ``connect.jdbc.sink.error.policy`` is set to **retry**.

``connect.jdbc.sink.mode``


Specifies how the data should be landed into the RDBMS. Two options are supported: **insert** **upsert**. 

* Type: string
* Importance: high
* Default: insert

``connect.jdbc.sink.export.mappings``

Specifies to the mappings of topic to table. Additionally which fields to select from the source topic and their mappings
to columns in the target table. Multiple mappings can set comma separated wrapped in {}.

Examples:

.. sourcecode:: bash

    {TOPIC1:TABLE1;field1->col1,field5->col5,field7->col10}
    {TOPIC2:TABLE2;field1->,field2->}
    {TOPIC3:TABLE3;*}

* Type: string
* Importance: high

.. warning::

    Explicit mapping of topics to tables is required. If not present the sink will not start and fail validation checks.

.. note::

    Specifying * for field mappings means select and try to map all fields in the topic to matching fields in the target
    table. Leaving the column name empty means trying to map to a column in the target table with the same name as the
    field in the source topic.

Example Configurations
~~~~~~~~~~~~~~~~~~~~~~

The below example gives a typical example, specifying the connection details, error policy and if batching is enabled.
The most complicated option is the ``connect.jdbc.sink.export.map``. This example has three mappings.

.. sourcecode:: bash

    #Name for the sink connector, must be unique in the cluster
    name=jdbc-datamountaineer-1
    #Name of the Connector class
    connector.class=com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcSinkConnector
    #Maximum number of tasks the Connector can start
    tasks.max=5
    #Input topics (Required by Connect Framework)
    topics=orders,otc_trades,greeks,bloomberg_prices
    #Target database connection URI, MUST INCLUDE DATABASE
    connect.jdbc.connection.uri=jdbc:mariadb://mariadb.landoop.com:3306/jdbc_sink_03
    #Target database username and password
    connect.jdbc.connection.user=testjdbcsink
    connect.jdbc.connection.password=datamountaineers
    #Location of the JDBC jar
    connect.jdbc.sink.driver.jar=/home/datamountaineers/connect-jdbc/connect/connect-hdfs-to-jdbc-wip/mariadb-java-client-1.4.4.jar
    #Name of the JDBC Driver class to load
    connect.jdbc.sink.driver.manager.class=org.mariadb.jdbc.Driver
    #Error policy to handle failures (default is ``throw``)
    connect.jdbc.sink.error.policy=THROW
    #Enable batching, all records the a task receives each time it's called are batched together in one transaction
    connect.jdbc.sink.batching.enabled=true
    #The topic to table mappings
    connect.jdbc.sink.export.map={orders:orders_table;product->product,qty->quantity,price->},{otc_trades:trades;*},{bloomberg_prices:prices;source->,lst_bid->}

For the first mapping **{orders:orders_table;product->product,qty->quantity,price->}** tells the sink the following:

1. Map the *orders* topic to a table called *orders_table*.
2. Select fields *product*, *qty* and *price*  from the topic.
3. Map a field called *product*  from the *orders*  topic to a column called *product* in the *orders_table*.
4. Map a field called *qty*  from the *orders*  topic to a column called *quantity* in the *orders_table*.
5. Map a field called *price*  from the *orders*  topic to a column called *price* in the *orders_tables*. Here the target column
   is left blank to the field name is taken.

For the second mapping **{otc_trades:trades;*}** tells the sink the following:

1. Map the *otc_trades* topic to a table called *trades*.
2. Select all fields from the topics message and map them against matching column names in the trades table.
   **The * indicates select all fields from the topic.**

The final mapping **{bloomberg_prices:prices;source->,lst_bid->}** tells the sink the following:

1. Map the *bloomberg_prices* topic to a table called *prices*.
2. Select fields *source*  and *lst_bid*  from the topic.
3. Map a field called *source*  from the *bloomberg_prices*  topic to a column called *source*  in the *prices* table.
4. Map a field called *lst_bid*  from the *bloomberg_prices*  topic to a column called *lst_bid*  in the *prices* table.

.. note::

    Specifying * for field mappings means select and try to map all fields in the topic to matching fields in the target table.
    Leaving the column name empty means trying to map to a column in the target table with the same name as the field in the source topic.

Schema Evolution
----------------

TODO

Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
