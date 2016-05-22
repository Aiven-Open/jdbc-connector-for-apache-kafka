Kafka Connect JDBC Sink
=======================

Kafka Connect JDBC Sink is a connector to write data from Kafka to a sink target that supports JDBC.

.. toctree::
    :maxdepth: 3

Prerequisites
-------------

-  Confluent 2.0.1
-  Java 1.7


Sink Connector QuickStart
-------------------------


To see some of the features of the JDBC we will write data from a topic to a SQLLite database, in INSERT mode and use
the RETRY functionality to recover from errors on insert.

.. note::

    You can use your favorite database instead of SQLite. Follow the same steps, but adjust the connection.url setting
    for your database. Confluent Platform includes JDBC drivers for SQLite and PostgreSQL, but if you’re using a
    different database you’ll also need to make sure the JDBC driver is available on the Kafka Connect process’s
    CLASSPATH.

Start by creating a database (you’ll need to install SQLite if you haven’t already):

.. sourcecode:: bash

    ➜  ~ sqlite3 test.db
        SQLite version 3.8.10.2 2015-05-20 18:17:19
        Enter ".help" for usage hints.
        sqlite> create table orders (id int, created varchar(150), product text, quantity int, price float, PRIMARY KEY (id))
        sqlite>

Now we create a configuration file that will load data from this database. This file is included with the connector in
``etc/kafka-connect-jdbc/quickstart-sqlite-sink.properties`` and contains the following settings:

.. sourcecode:: bash

    name=jdbc-datamountaineer-1
    connector.class=com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcSinkConnector
    tasks.max=1
    topics=orders
    connect.jdbc.connection.uri=jdbc:sqlite:test.db
    connect.jdbc.connection.user=
    connect.jdbc.connection.password=
    connect.jdbc.sink.error.policy=RETRY
    connect.jdbc.sink.batching.enabled=true
    connect.jdbc.sink.export.mappings={orders:orders;qty->quantity,product->,price->}
    connect.jdbc.sink.mode=INSERT

This configuration defines source topic ``topics``, the connection to target database  ``connect.jdbc.connection.uri``,
the error policy ``connect.jdbc.sink.error.policy``, the mappings of topics/fields ``connect.jdbc.sink.export.mappings``
and the sink write mode ``connect.jdbc.sink.mode``.

Now, run the connector as a standalone Kafka Connect worker in another terminal (this assumes Avro settings and that
Kafka and the Schema Registry are running locally on the default ports):

.. sourcecode:: bash

    ➜ /bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties
    etc/kafka-connect-jdbc/quickstart-sqlite-sink.properties

Check the sink is up with no errors.

.. sourcecode:: bash

        ____        __        __  ___                  __        _
       / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
      / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
     / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
    /_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
           ______  ____  ______   _____ _       __
          / / __ \/ __ )/ ____/  / ___/(_)___  / /__  by Stefan Bocutiu
     __  / / / / / __  / /       \__ \/ / __ \/ //_/
    / /_/ / /_/ / /_/ / /___    ___/ / / / / / ,<
    \____/_____/_____/\____/   /____/_/_/ /_/_/|_|
     (com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcSinkTask:56)
      INFO JdbcSinkConfig values:
        connect.jdbc.connection.user =
        connect.jdbc.sink.mode = INSERT
        connect.jdbc.sink.batching.enabled = true
        connect.jdbc.sink.error.policy = RETRY
        connect.jdbc.sink.max.retries = 10
        connect.jdbc.connection.password = [hidden]
        connect.jdbc.connection.uri = jdbc:sqlite:/Users/andrew/test.db
        connect.jdbc.sink.export.mappings = {orders:orders;qty->quantity,product->,price->}
        connect.jdbc.sink.retry.interval = 60000

.. tip::

    We try to catch all configuration errors at start and fail fast. Check the sink is up and not throwing configuration
    errors.

Next we need to add data to the orders topic we asked the sink to drain. Start the avro console producer:

.. sourcecode:: bash

    ➜ bin/kafka-avro-console-producer \
     --broker-list localhost:9092 --topic orders \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"},
     {"name":"product", "type": "string"}, {"name":"qty", "type": "int"}, {"name":"price", "type": "float"}]}'

The producer console is now waiting for input. Copy and paste the following into the terminal:

.. sourcecode:: bash

    ➜ {"id": 999, "product": "foo", "qty": 100, "price": 50}

In our mappings we wanted ``qty`` to go to ``quantity`` in our table. Back in the logs on the sink you should see this:

.. sourcecode:: bash

    INFO org.apache.kafka.connect.runtime.WorkerSinkTask@7fcca7e Committing offsets
    INFO Received 1 records. First entry topic:orders  partition:0 offset:0. Writing them to the database...
    INFO Finished writing 1 records to the database.

Check Sqlite:

.. sourcecode:: bash

    sqlite> select * from orders;
            999||foo|100|50.0

Since we have set our error policy to RETRY we can test to see what happens if a second record is inserted with the same
primary key. Back at the producer console insert the same record again which will cause a primary key violation:

.. sourcecode:: bash

    ➜ {"id": 999, "product": "foo", "qty": 100, "price": 50}


You should now see a primary key constraint violation and the sink pausing and retrying:

.. sourcecode:: bash

    ERROR An error has occurred inserting data starting at topic: orders offset: 1 partition: 0
    WARN Error policy set to RETRY. The following events will be replayed. Remaining attempts 9
    WARN Going to retry inserting data starting at topic: orders offset: 1 partition: 0

    ERROR RetriableException from SinkTask jdbc-datamountaineer-1-0:
    org.apache.kafka.connect.errors.RetriableException: java.sql.SQLException: UNIQUE constraint failed: orders.id

No lets fix this and have the sink recover. Connect to Sqlite again and delete the row:

.. sourcecode:: bash

    sqlite> delete from orders;


If you check the logs of the sink you will see it recover and write the row:

.. sourcecode:: bash

    INFO org.apache.kafka.connect.runtime.WorkerSinkTask@7fcca7e Committing offsets
    INFO Received 1 records. First entry topic:orders  partition:0 offset:1. Writing them to the database...
    INFO Recovered from exception "UNIQUE constraint failed: orders.id" at 2016-05-19 10:55:55.010Z. Continuing to process...
    INFO Finished writing 1 records to the database.
    INFO org.apache.kafka.connect.runtime.WorkerSinkTask@7fcca7e Committing offsets

Check Sqlite:

.. sourcecode:: bash

    sqlite> select * from orders;
            999||foo|100|50.0


.. note::

    The RETRY error handling is intended to allow operators to fix issues on databases without having to shutdown the
    connectors, for example, a DBA could be alerted and fix an issue without having to know about operating a connector.


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

Any error on write to the target database will be propagated up and processing is stopped. This is the default
behaviour.

**Noop**.

Any error on write to the target database is ignored and processing continues.

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

In this mode the sink prepares insert statements to execute either in batch transactions or individually, dependent on
the ``connect.jdbc.sink.batching.enabled`` setting. Typically you would use this in append only tables such as ledgers.
Combined with the error policy setting, ``connect.jdbc.sink.error.policy``, this allows for idempotent writes. For
example, sent to NOOP, violations of primary keys would be rejected by the database and sink would log the error and
continue processing but you miss real errors.

**Update**

In this mode the sink prepares upsert statements, the exact syntax is dependent on the target database.
The SQL dialect is obtained from the connection URI. When the sink tries to write, it executes the appropriate upsert
statement. For example, with MySQL it will use the
`ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_ to apply an update if a primary key
constraint is violated. If the update fails the sink fails back to the error policy.

The following dialects and upsert statements are supported:

1.  MySQL - `ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_
2.  ORACLE - `MERGE <https://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_9016.htm>`_.
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queried.
3.  MSSQL - `MERGE <https://msdn.microsoft.com/en-us/library/bb510625.aspx>`_.
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queried.
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queried.
    to retrieve this.
4.  PostgreSQL - *9.5 and above.* `ON CONFLICT <http://www.postgresql.org/docs/9.5/static/sql-insert.html>`_.
    This requires knowledge for the primary keys to build the merge statement. The database metadata is queried.


.. warning:: Postgre UPSERT is only supported on versions 9.5 and above.

Topic Routing
~~~~~~~~~~~~~

The sink supports topic routing that allows mapping the messages from topics to a specific table. For example map
a topic called "bloomberg_prices" to a table called "prices". This mapping is set in the
``connect.jdbc.sink.export.mappings`` option.

.. tip::

    Explicit mapping of topics to tables is required. If not present the sink will not start and fail validation checks.

Field Selection
~~~~~~~~~~~~~~~

The sink supports selecting fields from the source topic or selecting all fields and mapping of these fields to columns
in the target table. For example, map a field called "qty"  in a topic to a column called "quantity" in the target
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
    field mappings it is assumed the user is not interested in new upstream fields. For example they may be tapping into a
    pipeline for a Kafka stream job and not be intended as the final recipient of the stream.

    If a upstream field is removed and the topic is not following the Schema Registry's evolution rules, i.e. not
    full or backwards compatible, any errors will default to the error policy. If schema evolution rules have been followed,
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
driver for your database is not included, you will need to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector. The packaged connector is installed in the
``share/java/kafka-connect-jdbc`` directory, relative to the installation directory. If you have installed from Debian
or RPM packages, the connector will be installed in ``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or
tar files, the connector will be installed in the path given above under the directory where you unzipped the Confluent
Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running. For example:

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

* Type: password (shows ``[hidden]`` in logs)
* Importance: high

``connect.jdbc.sink.batching.enabled``

Specifies if a given sequence of SinkRecords are batched in a transaction or not. If ``true`` records delivered to
the task in each `put` are batched in one transaction. Otherwise each record is inserted in its own transaction.

* Type: boolean
* Importance: high
* Default : true


``connect.jdbc.sink.error.policy``

Specifies the action to be taken if an error occurs while inserting the data.

There are three available options, **noop**, the error is swallowed, **throw**, the error is allowed to propagate and retry.
For **retry** the Kafka message is redelivered up to a maximum number of times specified by the ``connect.jdbc.sink.max.retries``
option. The ``connect.jdbc.sink.retry.interval`` option specifies the interval between retries.

The errors will be logged automatically.

* Type: string
* Importance: high
* Default: ``throw``

``connect.jdbc.sink.max.retries``

The maximum number of times a message is retried. Only valid when the ``connect.jdbc.sink.error.policy`` is set to ``retry``.

* Type: string
* Importance: high
* Default: 10


``connect.jdbc.sink.retry.interval``

The interval, in milliseconds between retries if the sink is using ``connect.jdbc.sink.error.policy`` set to **RETRY**.

* Type: int
* Importance: medium
* Default : 60000 (1 minute)

``connect.jdbc.sink.mode``

Specifies how the data should be landed into the RDBMS. Two options are supported: **insert** **upsert**.

* Type: string
* Importance: high
* Default: insert

``connect.jdbc.sink.export.mappings``

Specifies to the mappings of topic to table. Additionally which fields to select from the source topic and their mappings
to columns in the target table. Multiple mappings can be set comma separated wrapped in {}. Before ``;`` is topic
to table mappings, after the field mappings.

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
    connect.jdbc.sink.export.mappings={orders:orders_table;product->product,qty->quantity,price->},{otc_trades:trades;*},{bloomberg_prices:prices;source->,lst_bid->}
    #write mode
    connect.jdbc.sink.mode=UPSERT

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

Schema Evolution
----------------

TODO

Deployment Guidelines
---------------------

TODO

TroubleShooting
---------------

TODO
