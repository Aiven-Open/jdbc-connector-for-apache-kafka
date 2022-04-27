# Aiven's JDBC Sink and Source Connectors for Apache Kafka®

This repository includes a Source connector that allows transfering data from a relational database into Apache Kafka topics
and a Sink connector that allows to transfer data from Kafka topics into a relational database
[Apache Kafka Connect](http://kafka.apache.org/documentation.html#connect) over JDBC.

The project originates from Confluent
[kafka-connect-jdbc](https://github.com/confluentinc/kafka-connect-jdbc).
The code was forked before the change of the project's license. We want
to thank the Confluent team for their efforts in developing the
connector and aim to continue keeping this fork well-maintained and
truly open.

## Documentation

The documentation can be found in the `docs/` directory of the source code

For the Source connector it could be found at
[here](docs/source-connector.md).

For the Sink connector it could be found at
[here](docs/sink-connector.md).

## Building from source

Prerequisites for building JDBC connector for Apache Kafka:

* Git
* Java 11

```
git clone git@github.com:aiven/jdbc-connector-for-apache-kafka.git
cd jdbc-connector-for-apache-kafka
./gradlew clean build
```

To publish to maven local use
```
./gradlew clean build publishToMavenLocal
```

## Contribute

[Source Code](https://github.com/aiven/jdbc-connector-for-apache-kafka)

[Issue Tracker](https://github.com/aiven/jdbc-connector-for-apache-kafka/issues)

## License

The project is licensed under the
[Apache 2 license](https://www.apache.org/licenses/LICENSE-2.0). See
[LICENSE](LICENSE).

## Security

Security policies can be found [here](SECURITY.md) and the list of known vulnerabilities [here](cve-list.md)

## Trademark
Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. Kafka Connect JDBC is trademark and property of their respective owners. All product and service names used in this page are for identification purposes only and do not imply endorsement.
