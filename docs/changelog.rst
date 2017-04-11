.. _jdbc_connector_changelog:

Changelog
=========

Version 3.2.1
-------------
No changes

Version 3.2.0
-------------

JDBC Source Connector
~~~~~~~~~~~~~~~~~~~~~
* `PR-156 <https://github.com/confluentinc/kafka-connect-jdbc/pull/156>`_ - CC-348: Separate username & password config for JDBC source connector
* `PR-155 <https://github.com/confluentinc/kafka-connect-jdbc/pull/155>`_ - Switch RST generation to ConfigDef.toEnrichedRst() available against 0.10.2
* `PR-154 <https://github.com/confluentinc/kafka-connect-jdbc/pull/154>`_ - Re-check stop flag before continuing after sleep
* `PR-167 <https://github.com/confluentinc/kafka-connect-jdbc/pull/167>`_ - Connection.isValid() expects seconds not milliseconds
* `PR-175 <https://github.com/confluentinc/kafka-connect-jdbc/pull/175>`_ - Use UTC Calendar when binding date/times in sink; fix thread-unsafe usage of UTC_CALENDAR in TimestampIncrementingTableQuerier
* `PR-169 <https://github.com/confluentinc/kafka-connect-jdbc/pull/169>`_ - Widen schema types for unsigned numeric types.

JDBC Sink Connector
~~~~~~~~~~~~~~~~~~~

* `PR-168 <https://github.com/confluentinc/kafka-connect-jdbc/pull/168>`_ - Change default MySQL Schema.Type.TIMESTAMP mapping from TIMESTAMP to DATETIME
* `PR-171 <https://github.com/confluentinc/kafka-connect-jdbc/pull/171>`_ - CC-432: Missing PreparedStatement.close()
* `PR-180 <https://github.com/confluentinc/kafka-connect-jdbc/pull/180>`_ - SqlServerDialect's upsert query placeholders should be in the order keyCols*, cols*


Version 3.1.1
-------------
No changes

Version 3.1.0
-------------

JDBC Source Connector
~~~~~~~~~~~~~~~~~~~~~

* `PR-148 <https://github.com/confluentinc/kafka-connect-jdbc/pull/148>`_ - Update licenses, tweaking some dependency scopes, files to include in packaging, and ensuring the create-licenses scope can run cleanly without extra manual steps.
* `PR-144 <https://github.com/confluentinc/kafka-connect-jdbc/pull/144>`_ - CC-263: Prevent retrying queries with a broken connection
* `PR-140 <https://github.com/confluentinc/kafka-connect-jdbc/pull/140>`_ - CC-331: Config option doc updates
* `PR-129 <https://github.com/confluentinc/kafka-connect-jdbc/pull/129>`_ - CC-311: support for Decimal logical type as incrementing column
* `PR-128 <https://github.com/confluentinc/kafka-connect-jdbc/pull/128>`_ - Fix short version in conf.py
* `PR-109 <https://github.com/confluentinc/kafka-connect-jdbc/pull/109>`_ - Ability to set the schema pattern for tables metadata retrieval
* `PR-122 <https://github.com/confluentinc/kafka-connect-jdbc/pull/122>`_ - supporting tinyint for primary key
* `PR-110 <https://github.com/confluentinc/kafka-connect-jdbc/pull/110>`_ - Added link to Confluent documentation for the connector.
* `PR-49 <https://github.com/confluentinc/kafka-connect-jdbc/pull/49>`_ - CC-69: support nanoseconds precision for timestamp-based offset tracking
* `PR-96 <https://github.com/confluentinc/kafka-connect-jdbc/pull/96>`_ - CC-243: use Long for nanos in offset map rather than Integer
* `PR-93 <https://github.com/confluentinc/kafka-connect-jdbc/pull/93>`_ - Fix thread-safety of date/time conversions in DataConverter
* `PR-87 <https://github.com/confluentinc/kafka-connect-jdbc/pull/87>`_ - Clean up table types documentation config and include it in a group with display attributes.
* `PR-37 <https://github.com/confluentinc/kafka-connect-jdbc/pull/37>`_ - Exposed Table Types as a config
* `PR-85 <https://github.com/confluentinc/kafka-connect-jdbc/pull/85>`_ - Add table.blacklist display name

JDBC Sink Connector
~~~~~~~~~~~~~~~~~~~

New in 3.1.0

Version 3.0.1
-------------

JDBC Source Connector
~~~~~~~~~~~~~~~~~~~~~

* `PR-88 <https://github.com/confluentinc/kafka-connect-jdbc/pull/88>`_ - Close all ResultSets in JdbcUtils
* `PR-94 <https://github.com/confluentinc/kafka-connect-jdbc/pull/94>`_ - add version.txt to share/doc

Version 3.0.0
-------------

JDBC Source Connector
~~~~~~~~~~~~~~~~~~~~~

* `PR-73 <https://github.com/confluentinc/kafka-connect-jdbc/pull/73>`_ - Update doc for CP 3.0.
* `PR-66 <https://github.com/confluentinc/kafka-connect-jdbc/pull/66>`_ - Task config should not show up in connector config.
* `PR-59 <https://github.com/confluentinc/kafka-connect-jdbc/pull/59>`_ - Add schema evolution in doc.
* `PR-55 <https://github.com/confluentinc/kafka-connect-jdbc/pull/55>`_ - Use new config definition.
* `PR-53 <https://github.com/confluentinc/kafka-connect-jdbc/pull/53>`_ - Adding checkstyle checks and the traditional minor fixes related.
* `PR-51 <https://github.com/confluentinc/kafka-connect-jdbc/pull/51>`_ - Add config to disable non-null checks.
* `PR-50 <https://github.com/confluentinc/kafka-connect-jdbc/pull/50>`_ - Bump version to 3.0.0-SNAPSHOT and Kafka dependency to 0.10.0.0-SNAPSHOT.
* `PR-48 <https://github.com/confluentinc/kafka-connect-jdbc/pull/48>`_ - Delayed copy.
* `PR-45 <https://github.com/confluentinc/kafka-connect-jdbc/pull/45>`_ - Added some logging.
