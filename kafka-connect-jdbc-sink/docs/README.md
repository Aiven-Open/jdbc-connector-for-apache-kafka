![](../images/DM-logo.jpg)

# Kafka Connect JDBC Sink

Kafka Connect JDBC Sink is a connector to write data from Kafka to a sink target that supports JDBC.

## Prerequisites

* Confluent 2.0.1
* Java 1.7

## Setup


## Sink Connector QuickStart

### Test Data

### Sink Connector Configuration


```bash 
```

### Starting the Sink Connector (Distributed)

A start in distributed mode.

```bash
➜  bin/connect-distributed \
➜  etc/schema-registry/connect-avro-distributed.properties 
```

Once the connector has started lets use the kafka-connect-tools cli to post in our distributed properties file.


```bash
➜  java -jar build/libs/kafka-connect-cli-0.2-all.jar \
➜  create jdbc-sink < jdbc-sink-distributed-orders.properties 

???????
```

Now check the logs to see if we started the sink.

```bash
```

Now check the database

```bash

```


## Features

The JDBC Sink Connector supports copying data from Kafka topics to databases with JDBC support. Testing has been against MySQL but providing the target sink support complies with the JDBC interface and ANSI 92 insert and update it should work against a most vendors. It constructs standard SQL insert and update statements.

The sink offers two main modes, **insert** and **update**, configurable via the ``connect.jdbc.sink.mode`` option. Additionaly the sink supports error polices in support of these modes.

Table to topic mapping is also supported via the ``connect.jdbc.topic.table.map`` option .i.e. write topicA to tableB as is field selection and aliasing via ``connect.jdbc.sink.fields``. This can be used to decide which fields to extract from the kafka topic to write the target table. See the [JDBC Sink Connector Configuration](### JDBC Sink Connector Configuration Options) section for more information.

### Error Polices

Error polices determine how to handle expections from the JDBC drivers during inserts or updates in the writers. Error policies are defined by the configuration option ``connect.jdbc.sink.error.policy`` and have four possible settings:

* ``throw`` - The default in which the exception is thrown, logged and processing stops.
* ``noop`` - The error is logged but no exception thrown. This is intended for use with database features such as unique indexes, primary keys and contraints to ensure idempotency and exactly once delivery. Also it can be used to simply ignore and keep processing.
* ``log\_to\_table`` - Log the error to a generic logging table.
* ``log\_to\_kafka`` - Log the error to a kafka topic.


### Insert Mode

In insert append only mode, records can be batched together in a single prepared statement or each record can be bound in it's own. 

If ``connect.jdbc.sink.batching.enabled`` is enabled all records delivered by the framework in task::poll are batched and submitted in one prepared statement, any failure causes the commit to be rolled back. If ``connect.jdbc.sink.batching.enabled`` is not enabled each record is bound and submitted individually and only it is rolled back on failure. How the failure is handled is dependent on the ``connect.jdbc.sink.error.policy`` option described earlier.

Idempotency and exactly once delivery can be achieved by using database features such as contraints and error polices. Setting the ``connect.jdbc.sink.error.policy`` to **noop** means inserts, for example, failing on a primary key violation would cause the statement to fail and be rolled back. The offending record would be logged and discarded. Failed statements are still logged even when ``connect.jdbc.sink.error.policy`` is set to **noop**. Delivering the same record twice from Kafka Connect framework would cause the second to be thrown away in this scenario.

.note:: ! Review the logs, error tables/topics to verify any failed inserts.

### Update mode

If the ``connect.jdbc.sink.mode`` is set to **update** and the insert fails and update is performed. If the update fails the error is handled accoring to the ``connect.jdbc.sink.error.policy`` setting. The update will only be applied if the error code is integrity constraint related.

.note:: ! Any update will only be performed if the [SQLException#getSQLState()](http://docs.oracle.com/javase/6/docs/api/java/sql/SQLException.html#getSQLState%28%29) starts with 23 (integrity constraint violation) 

## Configuration


The JDBC connector gives you quite a bit of flexibility in the databases you can export data to and how that data is exported. This section first describes how to access databases whose drivers are not included with Confluent Platform, then gives a few example configuration files that cover common scenarios, then provides an exhaustive description of the available configuration options.

### JDBC Drivers

The JDBC connector implements the data copying functionality on the generic JDBC APIs, but relies on JDBC drivers to handle the database-specific implementation of those APIs. Confluent Platform ships with a few JDBC drivers, but if the driver for your database is not included you will need to make it available via the ``CLASSPATH``.

One option is to install the JDBC driver jar alongside the connector. The packaged connector is installed in the ``share/java/kafka-connect-jdbc`` directory, relative to the installation directory. If you have installed from Debian or RPM packages, the connector will be installed in ``/usr/share/java/kafka-connect-jdbc``. If you installed from zip or tar files, the connector will be installed in the path given above under the directory where you unzipped the Confluent Platform archive.

Alternatively, you can set the ``CLASSPATH`` variable before running For example:

```bash
$ CLASSPATH=/usr/local/firebird/* ./bin/copycat-distributed ./config/copycat-distributed.properties
```
would add the JDBC driver for the Firebird database, located in ``/usr/local/firebird``, and allow you to use JDBC connection URLs like ``jdbc:firebirdsql:localhost/3050:/var/lib/firebird/example.db``.

### JDBC Sink Connector Configuration Options

``connect.jdbc.connection``

Specifies the database connection url. **Do not put username and password in this url as it will be printed in the logs when the Connectors validates as parses the configuration. Use the username and password configurations.**

  * Type: string
  * Importance: high
  
``connect.jdbc.topic.table.map``

Comma separated list of mappings from topics to tables. Each mapping is colon separated, if no table name is provided the topic name is used.

  * Type: string
  * Importance: high  

``connect.jdbc.sink.batching.enabled``

Specifies if for a given sequence of SinkRecords are batched or not.
True if the data is to be inserted in batch. False to create a sql statement for each record.

  * Type: boolean
  * Default: true
  * Importance: low  

``connect.jdbc.sink.driver.jar``

Specifies the jar file to be loaded at runtime containing the jdbc driver.

  * Type: string
  * Importance: high  

``connect.jdbc.sink.driver.manager.class``

Specifies the canonical class name for the driver manager.

  * Type: string
  * Importance: high  

``connect.jdbc.sink.fields``

Specifies which fields to consider when inserting the new JDBC entry.
If is not set it will use insert all the payload fields present in the payload.

Field mapping is supported; this way an avro record field can be inserted into a 'mapped' column.

e.g. fields to be used:field1,field2,field3
fields with mapping: field1=alias1,field2,field3=alias3

  * Type: string
  * Default: "*" //all
  * Importance: high  

``connect.jdbc.sink.error.policy``

Specifies the action to be taken if an error occurs while inserting the data.There are two available options: **noop** - the error is swallowed **throw** - the error is allowed to propagate. The error will be logged automatically.

  * Type: string
  * Default: "throw"
  * Importance: low  

#### Example


## Schema Evolution

TODO

## Deployment Guidelines

TODO

## TroubleShooting

TODO


