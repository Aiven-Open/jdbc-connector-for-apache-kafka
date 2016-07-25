Kafka Connect JDBC Sink (TODO out-of-date doc: updates to come)
================================================================

The JDBC sink allows for writing data from Kafka topics to an RDBMS. It has been tested against MYSQL, Oracle, Postgres
and SQL Server.

Prerequisites
-------------

-  Confluent 2.0.1
-  Java 1.7


Sink Connector QuickStart
-------------------------


To see some of the features of the JDBC we will write data from a topic to a SQLLite database in INSERT mode.

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
    connector.class=io.confluent.connect.jdbc.sink.JdbcSinkConnector
    tasks.max=1
    topics=orders
    connect.jdbc.connection.uri=jdbc:sqlite:test.db
    connect.jdbc.connection.user=
    connect.jdbc.connection.password=
    connect.jdbc.sink.error.policy=RETRY
    connect.jdbc.sink.export.mappings=INSERT INTO orders SELECT qty AS quantity, product, price FROM orders

This configuration defines the source topics ``topics``, the connection to target database  ``connect.jdbc.connection.uri``,
the error policy ``connect.jdbc.sink.error.policy``, the mappings of topics/fields ``connect.jdbc.sink.export.mappings``
and the sink write mode. The sink selects only qty, product and price from the orders topic and
sends them to the orders table.  The qty field from the topic is remapped as quantity in the target table.

Now, run the connector as a standalone Kafka Connect worker in another terminal:

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
        connect.jdbc.sink.export.mappings = INSERT INTO orders SELECT qty AS quantity, product, price FROM orders

.. tip::

    We try to catch all configuration errors at start and fail fast. Check the sink is up and not throwing configuration
    errors.

Next we need to add data to the orders topic we asked the sink to drain. Start the avro console producer:

.. sourcecode:: bash

    ➜ bin/kafka-avro-console-producer \
     --broker-list localhost:9092 --topic orders \
     --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"id","type":"int"},{"name":"product", "type": "string"}, {"name":"qty", "type": "int"}, {"name":"price", "type": "float"}]}'

The producer console is now waiting for input. Copy and paste the following into the terminal:

.. sourcecode:: bash

    ➜ {"id": 999, "product": "foo", "qty": 100, "price": 50}

In our mappings we wanted ``qty`` to be mapped to ``quantity`` in our table.

If you go back to the Sink logs you will see 1 row being inserted:

.. sourcecode:: bash

    INFO org.apache.kafka.connect.runtime.WorkerSinkTask@7fcca7e Committing offsets
    INFO Received 1 records. First entry topic:orders  partition:0 offset:0. Writing them to the database...
    INFO Finished writing 1 records to the database.

Check Sqlite:

.. sourcecode:: bash

    sqlite> select * from orders;
            999||foo|100|50.0

Since we have set our error policy to RETRY we can test to see what happens if a second record is inserted with the same
primary key. Back at the kafka producer console insert the same record again which will cause a primary key violation:

.. sourcecode:: bash

    ➜ {"id": 999, "product": "foo", "qty": 100, "price": 50}


You should now see a primary key constraint violation and the sink pausing and retrying:

.. sourcecode:: bash

    ERROR An error has occurred inserting data starting at topic: orders offset: 1 partition: 0
    WARN Error policy set to RETRY. The following events will be replayed. Remaining attempts 9
    WARN Going to retry inserting data starting at topic: orders offset: 1 partition: 0

    ERROR RetriableException from SinkTask jdbc-datamountaineer-1-0:
    org.apache.kafka.connect.errors.RetriableException: java.sql.SQLException: UNIQUE constraint failed: orders.id

No lets fix this and have the sink recover without our intervention in the sink. Connect to Sqlite again and delete the row:

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
2. Kafka connect query language.
3. Write modes.
4. Topic to table mappings.
5. Field Selection.
6. Auto create tables.
7. Auto evolve tables.

Error Polices
~~~~~~~~~~~~~

The sink has three error policies that determine how failed writes to the target database are handled. The error policies
affect the behaviour of the schema evolution characteristics of the sink. See the schema evolution section for more
information.

**Throw**

Any error on write to the target database will be propagated up and processing is stopped. This is the default
behaviour.

**Noop**

Any error on write to the target database is ignored and processing continues.

.. warning::

    This can lead to missed errors if you don't have adequate monitoring. Data is not lost as it's still in Kafka
    subject to Kafka's retention policy. The sink currently does **not** distinguish between integrity constraint
    violations and or SQL expections thrown by the driver,

**Retry**

Any error on write to the target database causes the RetryIterable exception to be thrown. This causes the
Kafka connect framework to pause and replay the message. Offsets are not committed. For example, if the table is offline
it will cause a write failure, the message can be replayed. With the Retry policy the issue can be fixed without stopping
the sink.

The length of time the sink will retry can be controlled by using the ``connect.jdbc.sink.max.retries`` and the
``connect.jdbc.sink.retry.interval``.


Kafka Connect Query Language
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**K** afka **C** onnect **Q** uery **L** anguage found here `GitHub repo <https://github.com/datamountaineer/kafka-connector-query-language>`_
allows for routing and mapping using a SQL like syntax, consolidating typically features in to one configuration option.

The JDBC sink supports the following:

.. sourcecode:: bash

    <write mode> INTO <target table> SELECT <fields> FROM <source topic> <AUTOCREATE> <PK> <PK_FIELDS> <AUTOEVOLVE>

Example:

.. sourcecode:: sql

    #Insert mode, select all fields from topicA and write to tableA
    INSERT INTO tableA SELECT * FROM topicA

    #Insert mode, select 3 fields and rename from topicB and write to tableB
    INSERT INTO tableB SELECT x AS a, y AS b and z AS c FROM topicB

    #Insert mode, select all fields from topicC, auto create tableC and auto evolve, default pks will be created
    INSERT INTO tableC SELECT * FROM topicC AUTOCREATE AUTOEVOLVE

    #Upsert mode, select all fields from topicC, auto create tableC and auto evolve, use field1 and field2 as the primary keys
    UPSERT INTO tableC SELECT * FROM topicC AUTOCREATE PK field1, field2 AUTOEVOLVE

Write Modes
~~~~~~~~~~~

The sink supports both **insert** and **upsert** modes.  This mapping is set in the ``connect.jdbc.sink.export.mappings`` option.

**Insert**

Insert is the default write mode of the sink. Records are batched by the sink and inserted into the target tables wrapped
in a transaction. Any errors occurring during writes are delegated to the error handler defined by the ``connect.jdbc.error.policy``.

**Update**

In this mode the sink prepares upsert statements, the exact syntax is dependent on the target database. The SQL dialect
is obtained from the connection URI. When the sink tries to write, it executes the appropriate upsert statement.
For example, with MySQL it will use the
`ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_ to apply an update if a primary
key constraint is violated. If the update fails the sink falls back to the error policy.

The following dialects and upsert statements are supported:

1.  MySQL - `ON DUPLICATE KEY <http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html>`_
2.  ORACLE - `MERGE <https://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_9016.htm>`_.
3.  MSSQL - `MERGE <https://msdn.microsoft.com/en-us/library/bb510625.aspx>`_.
4.  PostgreSQL - *9.5 and above.* `ON CONFLICT <http://www.postgresql.org/docs/9.5/static/sql-insert.html>`_.


.. note::

    Primary keys are required to be set on the target tables for upsert mode.

**Insert Idempotency**

Kafka currently provides at least once delivery semantics. Therefore, this mode may produce errors if unique constraints
have been implemented on the target tables. If the error policy has been set to NOOP then the sink will discard the error
and continue to process, however, it currently makes no attempt to distinguish violation of integrity constraints from other
SQLExceptions such as casting issues.

**Upsert Idempotency**

Kafka currently provides at least once delivery semantics and order is a guaranteed within partitions.

This mode will, if the same record is delivered twice to the sink, result in an idempotent write. The existing record
will be updated with the values of the second which are the same.

If records are delivered with the same field or group of fields that are used as the primary key on the target table,
but different values, the existing record in the target table will be updated.

Since records are delivered in the order they were written per partition the write is idempotent on failure or restart.
Redelivery produces the same result.


Topic Routing
~~~~~~~~~~~~~

The sink supports topic routing that allows mapping the messages from topics to a specific table. For example, map a
topic called "bloomberg_prices" to a table called "prices". This mapping is set in the ``connect.jdbc.sink.export.mappings``
option.

Example:

.. sourcecode:: sql

    //Select all
    INSERT INTO table1 SELECT * FROM topic1; INSERT INTO tableA SELECT * FROM topicC

.. tip::

    Explicit mapping of topics to tables is required. If not present the sink will not start and fail validation checks.

Field Selection
~~~~~~~~~~~~~~~

The JDBC sink supports field selection and mapping. This mapping is set in the ``connect.jdbc.sink.export.mappings`` option.


Examples:

.. sourcecode:: sql

    //Rename or map columns
    INSERT INTO table1 SELECT lst_price AS price, qty AS quantity FROM topicA

    //Select all
    INSERT INTO table1 SELECT * FROM topic1

.. tip:: Check your mappings to ensure the target columns exist.


.. warning::

    Field selection disables evolving the target table if the upstream schema in the Kafka topic changes. By specifying
    field mappings it is assumed the user is not interested in new upstream fields. For example they may be tapping into a
    pipeline for a Kafka stream job and not be intended as the final recipient of the stream.

    If you chose field selection you must include the primary key fields otherwise the insert will fail.

Auto Create Tables
~~~~~~~~~~~~~~~~~~

The sink supports auto creation of tables for each topic.

Any table auto created will have primary keys added. These can either be user specified fields from the topic schema or 3 default
columns set by the sink. If the defaults are requested the sink creates 3 columns, **__connect_topic**, **__connect_partition** and
**__connect_offset**. These columns are set as primary keys and used as such in insert and upsert modes. They are filled with the
topic name, partition and offset of the record they came from.

This mapping is set in the ``connect.jdbc.sink.export.mappings`` option.


Examples

.. sourcecode:: sql

    //AutoCreate the target table
    INSERT INTO table SELECT * FROM topic AUTOCREATE

    //AuoCreate the target table with USER defined PKS from the record
    INSERT INTO table SELECT * FROM topic AUTOCREATE PK field1, field2

..	note::

    The fields specified as the primary keys must be in the SELECT clause or all fields (*) must be selected

The sink will try and create the table at start up if a schema for the topic is found in the Schema Registry. If no
schema is found the table is created when the first record is received for the topic.

.. tip::

    Pre-create your topics with more than 1 partition to catch any DDL errors such as permission issues at startup!


Auto Evolve Tables
~~~~~~~~~~~~~~~~~~

Schema evolution can occur upstream, for example any new fields or change in data type in the schema of the topic, or
downstream DDLs on the database.

Upstream changes must follow the schema evolution rules laid out in the Schema Registry. This sink only supports BACKWARD
and FULLY compatible schemas. If new fields are added the sink will attempt to perform a ALTER table DDL statement against
the target table to add columns. All columns added to the target table are set as nullable.

Fields cannot be deleted upstream. Fields should be of Avro union type [null, <dataType>] with a default set. This allows
the sink to either retrieve the default value or null. The sink is not be aware that the field has been deleted
as a value is always supplied to it.

.. warning::

    If a upstream field is removed and the topic is not following the Schema Registry's  evolution rules, i.e. not full
    or backwards compatible, any errors will default to the error policy.

Downstream changes are handled by the sink. If columns are removed, the mapped fields from the topic are ignored. If
columns are added, we attempt to find a matching field by name in the topic.

Changes to data types can only be promotions.

This mapping is set in the ``connect.jdbc.sink.export.mappings`` option.

Example:

.. sourcecode:: sql

    UPSERT into EVOLUTION4 SELECT * FROM demo-evolution AUTOEVOLVE


.. tip::

    If you are adding columns to the target database set them a nullable and/or with a default value.

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

    $ CLASSPATH=/usr/local/firebird/* ./bin/connect-distributed ./config/connect-distributed.properties

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

``connect.jdbc.sink.batching.size``

Specifies how many records to insert together at one time. If the connect framework provides less records when it is
calling the sink it won't wait to fulfill this value but rather execute it.

* Type: int
* Importance: high
* Default: 3000


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
For unlimited retries set to -1.

* Type: string
* Importance: high
* Default: 10


``connect.jdbc.sink.retry.interval``

The interval, in milliseconds between retries if the sink is using ``connect.jdbc.sink.error.policy`` set to **RETRY**.

* Type: int
* Importance: medium
* Default : 60000 (1 minute)


``connect.jdbc.sink.export.mappings``

This mandatory configuration expects a KCQL statement specifing the source (topic) and target (table) mappings as well
as and field selections. Additionally AUTOCREATE (with and without primary keys) and AUTOEVOLVE can be set to control the
sinks behaviour. Multiple route mappings can be separated by a ``;``.


``connect.jdbc.sink.schema.registry.url``

The url for the Schema registry. This is used to retrieve the latest schema for table creation.

* Type : string
* Importance : high
* Default : http://localhost:8081


Example Configurations
~~~~~~~~~~~~~~~~~~~~~~

The below example gives a typical example, specifying the connection details, error policy and if batching is enabled.
The most complicated option is the ``connect.jdbc.sink.export.map``. This example has three mappings.

.. sourcecode:: bash

    #Name for the sink connector, must be unique in the cluster
    name=jdbc-datamountaineer-1
    #Name of the Connector class
    connector.class=io.confluent.connect.jdbc.sink.JdbcSinkConnector
    #Maximum number of tasks the Connector can start
    tasks.max=5
    #Input topics (Required by Connect Framework)
    topics=goldman_prices,bloomberg_prices
    #Target database connection URI
    connect.jdbc.connection.uri=jdbc:mariadb://mariadb.landoop.com:3306/jdbc_sink_03
    #Target database username and password
    connect.jdbc.connection.user=testjdbcsink
    connect.jdbc.connection.password=datamountaineers
    #Error policy to handle failures (default is ``throw``)
    connect.jdbc.sink.error.policy=THROW
   #The topic to table mappings
    connect.jdbc.sink.export.mappings=UPSERT INTO prices SELECT * FROM bloomberg_prices AUTOCREATE,UPSERT INTO prices SELECT * FROM
    goldman_prices AUTOCREATE

In this example we tell the sink to AUTOCREATE the prices table and use the default PKs, topic name, partition and offset.
We also tell the sink to map all fields in the Bloomberg prices and Goldman prices topic into the table prices and run in
UPSERT mode.

Deployment Guidelines
---------------------



TroubleShooting
---------------

**AutoCreate and AutoEvolve**

Ensure you have permissions to execute DDL statements against the database and target table.

**Tables not found**

The sink checks against the metadata of the target database if the tables exist at startup. Ensure the connection URI is
correct for your target database.

EXAMPLE connection strings.

.. sourcecode:: sql

    #MySQL
    jdbc:mysql://mariadb_host:3306/jdbc_sink_01?useServerPrepStmts=false&rewriteBatchedStatements=true

    #Postgress
    jdbc:postgresql://postgres_host:5432/jdbc_sink_01?currentSchema=public

    #Oracle
    jdbc:oracle:thin:@oracle_host:1521/XE:1521/XE

    #SQL Server
    jdbc:sqlserver://sqlserver_host:1433;databaseName=jdbc_sink_01


Oracle is case **sensitive** for table names. Wrap table names and columns in quotes to ensure the sink finds the tables.

**Duplicate Primary Keys**

If the sink is in RETRY error mode duplicate keys can still be an issue if within the batch of records the sink receives you
have duplicates. The sink batches records to write based on the ``connect.jdbc.sink.batch.size`` option. The error policy runs
at this batch level.