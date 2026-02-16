plugins {
    application
    id("java")
    `maven-publish`
    kotlin("jvm") version("2.3.10")
    kotlin("plugin.serialization") version("2.3.10")
}

group = "ben.gangl.lipson"
version = "1.0.0"

java {
    withSourcesJar()
}

repositories {
    mavenCentral()
}

val kotlinCoroutinesVersion = "1.7.3"
val kotlinSerializationJsonVersion = "1.10.0"
val kafkaClientsVersion = "3.9.1"
val logbackVersion = "1.5.13"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinSerializationJsonVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
}

tasks.test {
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        mavenLocal()
    }
}
