import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    application
    kotlin("jvm") version "1.7.0"
    id("com.github.johnrengelman.shadow") version "7.1.2"
}
configurations.all {
    exclude("org.slf4j", "commons-collections")
    exclude("org.slf4j", "slf4j-reload4j")
}
dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.4")

    implementation("com.github.airbytehq.json-avro-converter:converter:1.0.1")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.13.3")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.13.3")

    implementation("org.apache.logging.log4j:log4j-core:2.17.2")
    implementation("org.apache.logging.log4j:log4j-api:2.17.2")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
    implementation("org.slf4j:slf4j-api:1.7.32")
    // implementation("org.slf4j:slf4j-simple:1.7.32")

    implementation("org.apache.iceberg:iceberg-api:1.1.0")
    implementation("org.apache.iceberg:iceberg-core:1.1.0")
    implementation("org.apache.iceberg:iceberg-common:1.1.0")
    implementation("org.apache.iceberg:iceberg-data:1.1.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.1.0")
    implementation("org.apache.iceberg:iceberg-aws:1.1.0")
    implementation("org.apache.parquet:parquet-avro:1.12.3")
    implementation("org.apache.hadoop:hadoop-common:3.3.3") {
        exclude("org.slf4j")
    }
    implementation("org.apache.hadoop:hadoop-aws:3.3.3") {
        exclude("com.amazonaws")
    }
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.1026")
    implementation("com.amazonaws:aws-java-sdk-sts:1.11.1026")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.11.1026")
    implementation("com.amazonaws:aws-lambda-java-core:1.2.1")

    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.3")
    implementation("org.apache.parquet:parquet-hadoop-bundle:1.12.3")
    implementation("org.apache.parquet:parquet-avro:1.12.3")
    implementation("software.amazon.awssdk:glue:2.17.131") {
        exclude("software.amazon.awssdk", "apache-client")
        exclude("software.amazon.awssdk", "netty-nio-client")
    }
    implementation("software.amazon.awssdk:athena:2.17.131")
    implementation("software.amazon.awssdk:s3:2.17.131") {
        exclude("software.amazon.awssdk", "apache-client")
//        exclude("software.amazon.awssdk", "netty-nio-client")
    }
    implementation("software.amazon.awssdk:sts:2.17.131") {
        exclude("software.amazon.awssdk", "apache-client")
        exclude("software.amazon.awssdk", "netty-nio-client")
    }
    implementation("software.amazon.awssdk:url-connection-client:2.17.131")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.213")
    implementation("com.amazonaws:aws-lambda-java-events:3.11.0")
}

application {
    mainClass.set("com.matano.iceberg.IcebergMetadataWriterKt")
}

tasks {
    withType<ShadowJar> {
        isZip64 = true
//        minimize {
//            exclude(dependency("org.slf4j:.*:.*"))
//            exclude(dependency("org.apache.logging.log4j:.*:.*"))
//            exclude(dependency("software.amazon.awssdk:s3:.*"))
//            exclude(dependency("org.apache.iceberg:iceberg-core:.*"))
//        }
        archiveFileName.set("output.jar")
    }
}
