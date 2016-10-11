.. _sink-config-options:

JDBC Sink Configuration Options
-------------------------------

Connection
^^^^^^^^^^

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

Writes
^^^^^^

``insert.mode``
  The insertion mode to use. Supported modes are:

  ``insert``

      Use standard SQL ``INSERT`` statements.

  ``upsert``

      Use the appropriate upsert semantics for the target database if it is supported by the connector, e.g. ``INSERT OR IGNORE``.

  * Type: string
  * Default: insert
  * Valid Values: [insert, upsert]
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

.. _sink-pk-config-options:

``pk.mode``
  The primary key mode, also refer to ``pk.fields`` documentation for interplay. Supported modes are:

  ``none``

      No keys utilized.

  ``kafka``

      Kafka coordinates are used as the PK.

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

      Must be a trio representing the Kafka coordinates, defaults to ``__connect_topic,__connect_partition,__connect_offset`` if empty.

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
  Whether to automatically dd columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``.

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
