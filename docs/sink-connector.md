# Kafka Connect JDBC Sink Connector

This [Kafka Connect](https://kafka.apache.org/documentation/#connect)
connector allows you to transfer data from Kafka topics into a
relational database.

[Full configuration options reference](sink-connector-config-options.rst).

## How It Works

The connector subscribes to specified Kafka topics (`topics` or
`topics.regex` configuration, see
[the Kafka Connect documentation](https://kafka.apache.org/documentation/#connect_configuring))
and puts records coming from them into corresponding tables in the
database.

If record keys are used, they must be primitives or structs with
primitive fields.

Record values must be structs with primitive fields.

The connector requires knowledge of key and value schemas, so you should
use a converter with schema support, e.g., the JSON converter with
schema enabled.

It's possible to use a whitelist for record value fields by setting
`fields.whitelist`. If set, only the specified fields from a record's
value will be used. Note that the primary key fields are processed
separately (see [below](#primary-keys)).

## Database

### Connection

The connector is instructed how to connect to the database using
`connection.url`, `connection.user` and `connection.password`
configurations.

Some database drivers support SSL encryption of the connection, which is
configured with the connection URL as well.

The format of the connection URL is specific to the database driver.
Here are some documentations:
- [for PostgreSQL](https://jdbc.postgresql.org/documentation/head/connect.html);
- [for MySQL](https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html);
- [for MariaDB](https://mariadb.com/kb/en/library/about-mariadb-connector-j/#connection-strings);
- [for SQLite](http://www.sqlitetutorial.net/sqlite-java/sqlite-jdbc-driver/);
- [for Derby](https://db.apache.org/derby/docs/10.15/devguide/cdevdvlp17453.html).

For example, for PostgreSQL the connection URL might look like
```
jdbc:postgresql://localhost:5432/test?user=fred&password=secret&ssl=true
```

### SQL Dialects

Different databases use different dialects of SQL. The connector
supports several of them.

By default, the connector will automatically determine the dialect based
on the connection URL (`connection.url`).

If needed, it's possible to specify the dialect explicitly using
`dialect.name` configuration. Check
[the configuration's reference](source-connector-config-options.rst#database)
for the list of supported dialects.

## Insertion Modes

The connector has three insertion modes.

### Insert Mode

In this mode, the connector executes `INSERT` SQL query on each record
from Kafka.

This mode is used by default. To enable it explicitly, set
`insert.mode=insert`.

### Multi Mode

In this mode, the connector executes an `INSERT` SQL query with multiple
values (effectively inserting multiple row/records per query).

To use this mode, set `insert.mode=multi`

### Update Mode

In this mode, the connector executes `UPDATE` SQL query on each record
from Kafka.

To use this mode, set `insert.mode=update`.

### Upsert Mode

In this mode, the connector executes an SQL query commonly known as
_upsert_ (it has different names in different databases).

To use this mode, set `insert.mode=upsert`.

#### Write Idempotence

_Upsert_ provides the ability to atomically insert a row if there were
no conflicts on the primary key constraint or, in case of a conflict, to
update the existing row with the new data. This semantics provides
_write idempotence_, which may be desirable in many cases, such as:
- the source topic naturally containing multiple records with the same
  primary key;
- failure recovery when re-processing is involved.

#### Database Support

_Upsert_ is not a standard SQL feature and different databases might not
support it. Here is the list of databases that support _upsert_ in this
connector and the syntax they use for this:

|  Database  |                 Syntax used                 |
|:----------:|---------------------------------------------|
|    DB2     | `MERGE ..`                                  |
|   Derby    | `MERGE ..`                                  |
|    MySQL   | `INSERT .. ON DUPLICATE KEY REPLACE ..`     |
|   Oracle   | `MERGE ..`                                  |
| PostgreSQL | `INSERT .. ON CONFLICT .. DO UPDATE SET ..` |
|  SAP HANA  | `UPSERT ..`                                 |
|   SQLite   | `INSERT OR REPLACE ..`                      |
| SQL Server | `MERGE ..`                                  |
|   Sybase   | `MERGE ..`                                  |

The connector does not support other databases for _upsert_ at the
moment.

## Primary Keys

The connector supports several sources of the primary key values.

### No Primary Key

This is the simplest mode in which no primary key is used.

This mode is used by default. To enable it explicitly, set
`pk.mode=none`.

### Kafka Coordinates

In this mode, the connector uses Kafka coordinates—the topic, partition,
and offset—as a composite primary key.

It is possible to specify the names of the corresponding fields in the
destination table by configuring:
```
pk.fields=<topic_column>,<pratition_column>,<offset_column>
```
If not specified, `__connect_topic`, `__connect_partition`, and
`__connect_offset` will be used.

To use this mode, set `pk.mode=kafka`.

### Record Key

In this mode, the connector uses the record's key as the source of
primary key values. It may be a primitive or a structure.

If the record key is a primitive, only one field must be specified in
`pk.fields`, which will be used as the column name.

If the record key is a structure, all or some of its fields can be used.
By default, it is all. You can specify which fields to use by setting
`pk.fields`. Note that field name and the column name will be the same.

To use this mode, set `pk.mode=record_key`.

### Record Value

A record's value is supposed to be a structure and in this mode, the
connector uses the record's value's fields as the source of primary key
values.

By default, all the fields are used. You can specify which fields to use
by setting `pk.fields`. Note that field name and the column name will be
the same.

To use this mode, set `pk.mode=record_value`.

## Table Auto-Creation and Auto-Evolution

### Auto-Creation

The connector can automatically create the destination table if it does
not exist. Tables will be created as records are being consumed from
Kafka. The connector will use the record schema, the field whitelist (if
defined), and the primary key definitions to create the list of the
table columns.

To enable table auto-creation, set `auto.create=true`.

### Auto-Evolution

If the schema of records changes, the connector can perform limited
auto-evolution of the destination table by `ALTER` SQL queries. The
following auto-evolution limitations apply:
- The connector does not delete columns.
- The connector does not alter column types.
- The connector does not add primary keys constraints.

To enable table auto-evolution, set `auto.evolve=true`.

## Data Mapping

The nullability of a column is based on the optionality of the
corresponding fields in the schema.

The default values for columns are based on the default values of the
corresponding fields in the schema.

The following mappings from Connect schema types to database-specific
types are used.

| Connect schema type |         DB2         |        Derby        |        MySQL        |
|:-------------------:|:-------------------:|:-------------------:|:-------------------:|
|        `INT8`       |      `SMALLINT`     |      `SMALLINT`     |      `TINYINT`      |
|       `INT16`       |      `SMALLINT`     |      `SMALLINT`     |      `SMALLINT`     |
|       `INT32`       |      `INTEGER`      |      `INTEGER`      |        `INT`        |
|       `INT64`       |       `BIGINT`      |       `BIGINT`      |       `BIGINT`      |
|      `FLOAT32`      |       `FLOAT`       |       `FLOAT`       |       `FLOAT`       |
|      `FLOAT64`      |       `DOUBLE`      |       `DOUBLE`      |       `DOUBLE`      |
|      `BOOLEAN`      |      `SMALLINT`     |      `SMALLINT`     |      `TINYINT`      |
|       `STRING`      |   `VARCHAR(32672)`  |   `VARCHAR(32672)`  |    `VARCHAR(256)`   |
|       `BYTES`       |    `BLOB(64000)`    |    `BLOB(64000)`    |  `VARBINARY(1024)`  |
|      `Decimal`      | `DECIMAL(31,scale)` | `DECIMAL(31,scale)` | `DECIMAL(65,scale)` |
|        `Date`       |        `DATE`       |        `DATE`       |        `DATE`       |
|        `Time`       |        `TIME`       |        `TIME`       |      `TIME(3)`      |
|     `Timestamp`     |     `TIMESTAMP`     |     `TIMESTAMP`     |    `DATETIME(3)`    |

_(continued)_

| Connect schema type |       Oracle      |     PostgreSQL     |     SAP HANA    |
|:-------------------:|:-----------------:|:------------------:|:---------------:|
|        `INT8`       |   `NUMBER(3,0)`   |     `SMALLINT`     |    `TINYINT`    |
|       `INT16`       |   `NUMBER(5,0)`   |     `SMALLINT`     |    `SMALLINT`   |
|       `INT32`       |   `NUMBER(10,0)`  |        `INT`       |    `INTEGER`    |
|       `INT64`       |   `NUMBER(19,0)`  |      `BIGINT`      |     `BIGINT`    |
|      `FLOAT32`      |   `BINARY_FLOAT`  |       `REAL`       |      `REAL`     |
|      `FLOAT64`      |  `BINARY_DOUBLE`  | `DOUBLE PRECISION` |     `DOUBLE`    |
|      `BOOLEAN`      |   `NUMBER(1,0)`   |      `BOOLEAN`     |    `BOOLEAN`    |
|       `STRING`      |       `CLOB`      |       `TEXT`       | `VARCHAR(1000)` |
|       `BYTES`       |       `BLOB`      |       `BYTEA`      |      `BLOB`     |
|      `Decimal`      | `NUMBER(*,scale)` |      `DECIMAL`     |    `DECIMAL`    |
|        `Date`       |       `DATE`      |       `DATE`       |      `DATE`     |
|        `Time`       |       `DATE`      |       `TIME`       |      `DATE`     |
|     `Timestamp`     |    `TIMESTAMP`    |     `TIMESTAMP`    |   `TIMESTAMP`   |

_(continued)_

| Connect schema type |   SQLite  |      SQL Server     |
|:-------------------:|:---------:|:-------------------:|
|        `INT8`       | `INTEGER` |      `TINYINT`      |
|       `INT16`       | `INTEGER` |      `SMALLINT`     |
|       `INT32`       | `INTEGER` |         `INT`       |
|       `INT64`       | `INTEGER` |       `BIGINT`      |
|      `FLOAT32`      |   `REAL`  |        `REAL`       |
|      `FLOAT64`      |   `REAL`  |       `FLOAT`       |
|      `BOOLEAN`      | `INTEGER` |        `BIT`        |
|       `STRING`      |   `TEXT`  |    `VARCHAR(MAX)`   |
|       `BYTES`       |   `BLOB`  |   `VARBINARY(MAX)`  |
|      `Decimal`      | `NUMERIC` | `DECIMAL(38,scale)` |
|        `Date`       | `NUMERIC` |        `DATE`       |
|        `Time`       | `NUMERIC` |        `TIME`       |
|     `Timestamp`     | `NUMERIC` |     `DATETIME2`     |

_(continued)_

| Connect schema type |                          Sybase                         |       Vertica       |
|:-------------------:|:-------------------------------------------------------:|:-------------------:|
|        `INT8`       |                        `SMALLINT`                       |        `INT`        |
|       `INT16`       |                        `SMALLINT`                       |        `INT`        |
|       `INT32`       |                          `INT`                          |        `INT`        |
|       `INT64`       |                         `BIGINT`                        |        `INT`        |
|      `FLOAT32`      |                          `REAL`                         |       `FLOAT`       |
|      `FLOAT64`      |                         `FLOAT`                         |       `FLOAT`       |
|      `BOOLEAN`      |       `TINYINT` (nullable) or `BIT` (non-nullable)      |      `BOOLEAN`      |
|       `STRING`      | `VARCHAR(512)` (primary keys) or `TEXT` (other columns) |   `VARCHAR(1024)`   |
|       `BYTES`       |                         `IMAGE`                         |  `VARBINARY(1024)`  |
|      `Decimal`      |                   `DECIMAL(38,scale)`                   | `DECIMAL(38,scale)` |
|        `Date`       |                          `DATE`                         |        `DATE`       |
|        `Time`       |                          `TIME`                         |        `TIME`       |
|     `Timestamp`     |                        `DATETIME`                       |      `DATETIME`     |

## Example

Let's look at an example.

We have `messages` table in PostgreSQL with the following schema:

| Column name | Type        |
|:-----------:|:-----------:|
| `text`      | `VARCHAR`   |
| `sent_at`   | `TIMESTAMP` |

We have messages in JSON that look like this:
```json
{
  "text": "Hello",
  "sent_at": 1560507792000
}
```

where `1560507792000` is `Friday, June 14, 2019 10:23:12 AM` as a Unix
timestamp in milliseconds.

We want to ingest these messages into `messages` table using Kafka topic
with the same name `messages` and the JDBC Sink connector.

Let's set up the connector.

Kafka record values must be structs with primitive fields. This
is fine, our JSON structure perfectly fits this.

The converter requires the knowledge of the value schema. We will use
`org.apache.kafka.connect.json.JsonConverter` for values with enabled
schemas. However currently (as of Kafka 2.2.1) `JsonConverter` with
enabled schemas requires record values to contain explicit schemas in
themselves. In our case, this looks like this:
```json
{
  "schema": {
    "type": "struct",
    "fields": [
        { "field": "text", "type": "string", "optional": false },
        { "field": "sent_at", "type": "int64", "name": "org.apache.kafka.connect.data.Timestamp", "optional": false }
    ]
  },
  "payload": {
      "text": "Hello",
      "sent_at": 1560507792000
  }
}
```

Messages in this format should be published into `messages` topic.

Here's a configuration that makes this case work:

```properties
name=example-jdbc-sink

# These are defaults, but they're here for clarity:
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true

connector.class=io.aiven.connect.jdbc.JdbcSinkConnector
connection.url=jdbc:postgresql://localhost:5432/kafkaconnect?user=postgres&password=mysecretpassword

topics=messages

# This is default, but it's here for clarity:
insert.mode=insert
```
