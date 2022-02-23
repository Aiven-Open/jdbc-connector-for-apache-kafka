=========================================
JDBC Sink connector Configuration Options
=========================================

Database
^^^^^^^^

``connection.url``
  JDBC connection URL.

  * Type: string
  * Importance: high

``connection.user``
  JDBC connection user.

  * Type: string
  * Default: null
  * Importance: high

``connection.password``
  JDBC connection password.

  * Type: password
  * Default: null
  * Importance: high

``db.timezone``
  Name of the JDBC timezone that should be used in the connector when querying with time-based criteria. Defaults to UTC.

  * Type: string
  * Default: UTC
  * Valid Values: valid time zone identifier (e.g., 'Europe/Helsinki', 'UTC+2', 'Z', 'CET')
  * Importance: medium

``dialect.name``
  The name of the database dialect that should be used for this connector. By default this is empty, and the connector automatically determines the dialect based upon the JDBC connection URL. Use this if you want to override that behavior and use a specific dialect. All properly-packaged dialects in the JDBC connector plugin can be used.

  * Type: string
  * Default: ""
  * Valid Values: [, Db2DatabaseDialect, MySqlDatabaseDialect, SybaseDatabaseDialect, GenericDatabaseDialect, OracleDatabaseDialect, SqlServerDatabaseDialect, PostgreSqlDatabaseDialect, SqliteDatabaseDialect, DerbyDatabaseDialect, SapHanaDatabaseDialect, VerticaDatabaseDialect]
  * Importance: low

``sql.quote.identifiers``
  Whether to delimit (in most databases, quote with double quotes) identifiers (e.g., table names and column names) in SQL statements.

  * Type: boolean
  * Default: true
  * Importance: low

Writes
^^^^^^

``insert.mode``
  The insertion mode to use. Supported modes are:

  ``insert``

      Use standard SQL ``INSERT`` statements.

  ``multi``

      Use multi-row inserts, e.g. ``INSERT INTO table_name (column_list) VALUES (value_list_1), (value_list_2), ... (value_list_n);``

  ``upsert``

      Use the appropriate upsert semantics for the target database if it is supported by the connector, e.g. ``INSERT .. ON CONFLICT .. DO UPDATE SET ..``.

  ``update``

      Use the appropriate update semantics for the target database if it is supported by the connector, e.g. ``UPDATE``.

  * Type: string
  * Default: insert
  * Valid Values: [insert, multi, upsert, update]
  * Importance: high

``batch.size``
  Specifies how many records to attempt to batch together for insertion into the destination table, when possible.

  * Type: int
  * Default: 3000
  * Valid Values: [0,...]
  * Importance: medium

Data Mapping
^^^^^^^^^^^^

``table.name.format``
  A format string for the destination table name, which may contain '${topic}' as a placeholder for the originating topic name.

  For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name 'kafka_orders'.

  * Type: string
  * Default: ${topic}
  * Importance: medium

``table.name.normalize``
  Whether or not to normalize destination table names for topics. When set to ``true``, the alphanumeric characters (``a-z A-Z 0-9``) and ``_`` remain as is, others (like ``.``) are replaced with ``_``.

  * Type: boolean
  * Default: false
  * Importance: medium

``pk.mode``
  The primary key mode, also refer to ``pk.fields`` documentation for interplay. Supported modes are:

  ``none``

      No keys utilized.

  ``kafka``

      Kafka coordinates (the topic, partition, and offset) are used as the PK.

  ``record_key``

      Field(s) from the record key are used, which may be a primitive or a struct.

  ``record_value``

      Field(s) from the record value are used, which must be a struct.

  * Type: string
  * Default: none
  * Valid Values: [none, kafka, record_key, record_value]
  * Importance: high

``pk.fields``
  List of comma-separated primary key field names. The runtime interpretation of this config depends on the ``pk.mode``:

  ``none``

      Ignored as no fields are used as primary key in this mode.

  ``kafka``

      Must be a trio representing the Kafka coordinates (the topic, partition, and offset), defaults to ``__connect_topic,__connect_partition,__connect_offset`` if empty.

  ``record_key``

      If empty, all fields from the key struct will be used, otherwise used to extract the desired fields - for primitive key only a single field name must be configured.

  ``record_value``

      If empty, all fields from the value struct will be used, otherwise used to extract the desired fields.

  * Type: list
  * Default: ""
  * Importance: medium

``fields.whitelist``
  List of comma-separated record value field names. If empty, all fields from the record value are utilized, otherwise used to filter to the desired fields.

  Note that ``pk.fields`` is applied independently in the context of which field(s) form the primary key columns in the destination database, while this configuration is applicable for the other columns.

  * Type: list
  * Default: ""
  * Importance: medium

DDL Support
^^^^^^^^^^^

``auto.create``
  Whether to automatically create the destination table based on record schema if it is found to be missing by issuing ``CREATE``.

  * Type: boolean
  * Default: false
  * Importance: medium

``auto.evolve``
  Whether to automatically add columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``.

  * Type: boolean
  * Default: false
  * Importance: medium

Retries
^^^^^^^

``max.retries``
  The maximum number of times to retry on errors before failing the task.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: medium

``retry.backoff.ms``
  The time in milliseconds to wait following an error before a retry attempt is made.

  * Type: int
  * Default: 3000
  * Valid Values: [0,...]
  * Importance: medium


