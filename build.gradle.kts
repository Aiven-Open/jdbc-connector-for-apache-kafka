import java.io.FileOutputStream
import java.net.URL

/*
 * Copyright 2024 Aiven Oy and jdbc-connector-for-apache-kafka contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    // https://docs.gradle.org/current/userguide/java_library_plugin.html
    `java-library`

    // https://docs.gradle.org/current/userguide/checkstyle_plugin.html
    checkstyle

    // https://docs.gradle.org/current/userguide/jacoco_plugin.html
    jacoco

    // https://docs.gradle.org/current/userguide/distribution_plugin.html
    distribution

    // https://docs.gradle.org/current/userguide/publishing_maven.html
    `maven-publish`

    // https://docs.gradle.org/current/userguide/idea_plugin.html
    idea
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withSourcesJar()
}

tasks.wrapper {
    doFirst {
        val sha256Sum = URL("$distributionUrl.sha256").readText()
        propertiesFile.appendText("distributionSha256Sum=${sha256Sum}\n")
        println("Added checksum to wrapper properties")
    }
    distributionType = Wrapper.DistributionType.ALL
}

checkstyle {
    toolVersion = "10.16.0"
    configDirectory.set(rootProject.file("checkstyle/"))
}

jacoco {
    toolVersion = "0.8.12"
}


distributions {
    named("main") {
        contents {
            from(tasks.jar)
            from(configurations.runtimeClasspath)
            from(projectDir) {
                include("version.txt", "README*", "LICENSE*", "NOTICE*", "licenses/")
                include("config/")
            }
        }
    }
}

publishing {
    publications {
        register("maven", MavenPublication::class) {
            // Defaults, for clarity
            groupId = groupId
            artifactId = "jdbc-connector-for-apache-kafka"
            version = version

            from(components["java"])

            pom {
                name = "Aiven's JDBC Sink and Source Connectors for Apache Kafka"
                description = "A Kafka Connect JDBC connector for copying data between databases and Kafka."
                url = "https://aiven.io"
                organization {
                    name = "Aiven Oy"
                    url = "https://aiven.io"
                }
                licenses {
                    license {
                        name = "Apache License 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.html"
                        distribution = "repo"

                    }
                }
                scm {
                    connection = "scm:git:git://github.com/aiven/jdbc-connector-for-apache-kafka.git"
                    developerConnection = "scm:git:git@github.com:aiven/jdbc-connector-for-apache-kafka.git"
                    url = "https://github.com/aiven/jdbc-connector-for-apache-kafka"
                    tag = "HEAD"
                }
            }
        }
    }
}

val kafkaVersion = "3.3.2"
val slf4jVersion = "2.0.13"

val avroVersion = "1.8.1"
// Version 1.8.1 brings Jackson 1.9.x/org.codehaus.jackson package for Avro and Confluent Platform 4.1.4.
val confluentPlatformVersion = "4.1.4" // For compatibility tests use version 4.1.4.
val hamcrestVersion = "2.2"
val jacksonVersion = "2.17.2" // This Jackson is used in the tests.
val jupiterVersion = "5.11.0"
val servletVersion = "4.0.1"
val testcontainersVersion = "1.20.2"
val awaitilityVersion = "4.2.1"
val log4jVersion = "2.20.0"

// Integration tests are not necessary to change these
val derbyVersion = "10.15.2.0"
val mockitoVersion = "5.12.0"

sourceSets {
    create("integrationTest") {
        java.srcDir(file("src/integrationTest/java"))
        compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

idea {
    module {
        testSources.from(project.sourceSets["integrationTest"].java.srcDirs)
        testSources.from(project.sourceSets["integrationTest"].resources.srcDirs)
    }
}

val integrationTestImplementation: Configuration by configurations.getting {
    extendsFrom(configurations["testImplementation"])
}

val integrationTestRuntimeOnly: Configuration by configurations.getting {
    extendsFrom(configurations["testRuntimeOnly"])
}

dependencies {
    compileOnly("org.apache.kafka:connect-api:$kafkaVersion")

    runtimeOnly("org.xerial:sqlite-jdbc:3.46.0.1")
    runtimeOnly("org.postgresql:postgresql:42.7.3")
    runtimeOnly("com.oracle.database.jdbc:ojdbc8:23.4.0.24.05")
    runtimeOnly("net.sourceforge.jtds:jtds:1.3.1")
    runtimeOnly("net.snowflake:snowflake-jdbc:3.16.1")
    runtimeOnly("com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11")
    runtimeOnly("com.mysql:mysql-connector-j:8.4.0")

    implementation("com.google.guava:guava:33.2.1-jre")
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$jupiterVersion")
    testImplementation("org.junit.vintage:junit-vintage-engine:$jupiterVersion")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
    testImplementation("org.apache.kafka:connect-api:$kafkaVersion")
    testImplementation("commons-io:commons-io:2.16.1")
    testImplementation("org.apache.derby:derby:$derbyVersion")
    testImplementation("org.apache.derby:derbyclient:$derbyVersion")
    testImplementation("org.apache.derby:derbyshared:$derbyVersion")
    testImplementation("org.apache.derby:derbytools:$derbyVersion")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.awaitility:awaitility:4.2.1")

    testRuntimeOnly("org.slf4j:slf4j-log4j12:$slf4jVersion")

    integrationTestRuntimeOnly("io.confluent:kafka-avro-serializer:$confluentPlatformVersion")
    integrationTestRuntimeOnly("io.confluent:kafka-connect-avro-converter:$confluentPlatformVersion")
    integrationTestRuntimeOnly("io.confluent:kafka-json-serializer:$confluentPlatformVersion")
    integrationTestRuntimeOnly("org.slf4j:slf4j-log4j12:$slf4jVersion")

    integrationTestImplementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    integrationTestImplementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    integrationTestImplementation("javax.servlet:javax.servlet-api:$servletVersion")
    integrationTestImplementation("org.apache.avro:avro:$avroVersion")
    integrationTestImplementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    integrationTestImplementation("org.junit.jupiter:junit-jupiter:$jupiterVersion")
    integrationTestImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:kafka:$testcontainersVersion") // this is not Kafka version
    integrationTestImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:oracle-free:$testcontainersVersion")

    integrationTestImplementation("org.awaitility:awaitility:$awaitilityVersion")
    integrationTestImplementation("org.assertj:assertj-db:2.0.2")

    // Make test utils from 'test' available in 'integration-test'
    integrationTestImplementation(sourceSets["test"].output)
}

tasks.test {
    useJUnitPlatform()
}

tasks {
    create("integrationTest", Test::class) {
        description = "Runs the integration tests."
        group = "verification"
        testClassesDirs = sourceSets["integrationTest"].output.classesDirs
        classpath = sourceSets["integrationTest"].runtimeClasspath

        dependsOn(testClasses, distTar)

        useJUnitPlatform()

        // Run always.
        outputs.upToDateWhen { false }

        // Pass the distribution file path to the tests.
        systemProperty("integration-test.distribution.file.path", distTar.get().archiveFile.get().asFile.path)
    }
}

tasks.processResources {
    filesMatching("jdbc-connector-for-apache-kafka-version.properties") {
        expand(mapOf("version" to version))
    }
}

tasks.register("connectorConfigDoc") {
    description = "Generates the connector's configuration documentation."
    group = "documentation"
    dependsOn("classes")

    doLast {
        project.javaexec {
            mainClass = "io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig"
            classpath = sourceSets.main.get().runtimeClasspath + sourceSets.main.get().compileClasspath
            standardOutput = FileOutputStream("$projectDir/docs/source-connector-config-options.rst")
        }
        project.javaexec {
            mainClass = "io.aiven.connect.jdbc.sink.JdbcSinkConfig"
            classpath = sourceSets.main.get().runtimeClasspath + sourceSets.main.get().compileClasspath
            standardOutput = FileOutputStream("$projectDir/docs/sink-connector-config-options.rst")
        }
    }
}
