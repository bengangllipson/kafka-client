plugins {
    id("java")
    `maven-publish`
    kotlin("jvm") version("2.3.10")
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
val kafkaClientsVersion = "3.6.0"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinCoroutinesVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaClientsVersion")
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
