import java.net.URI

plugins {
    kotlin("jvm") version "1.5.20"
    `maven-publish`
}

repositories {
    mavenCentral()
}

subprojects {
    apply(plugin = "kotlin")
    apply(plugin = "maven-publish")
    repositories {
        mavenCentral()
        maven {
            url = URI("https://jitpack.io")
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        withSourcesJar()
    }

    kotlin {
        explicitApi()
    }

    dependencies {
        implementation(kotlin("stdlib"))

        testImplementation(kotlin("test"))
    }

    publishing {
        publications {
            create<MavenPublication>("maven") {
                groupId = "com.github.Jeff-Gillot.kafka-flow"
                artifactId = project.name
                version = "1.0.2-SNAPSHOT"

                from(components["java"])
            }
        }
    }
}

val kafkaVersion = "2.8.0"
project(":kafka-flow-topic-descriptor") {
    dependencies {
        implementation("com.sangupta:murmur:1.0.0")
    }
}

project(":kafka-flow-client") {
    dependencies {
        implementation(project(":kafka-flow-topic-descriptor"))
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
        implementation( "com.github.Jeff-Gillot:time-extension:1.0.0")
        testImplementation("io.strikt:strikt-core:0.31.0")
    }
}

project(":kafka-flow-testing") {
    dependencies {
        implementation(project(":kafka-flow-topic-descriptor"))
        implementation(project(":kafka-flow-client"))
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
        implementation("io.github.microutils:kotlin-logging-jvm:2.0.6")
        implementation("org.slf4j:slf4j-simple:1.7.29")
        implementation("org.testcontainers:testcontainers:1.15.3")
        implementation("org.testcontainers:kafka:1.15.3")
        implementation( "com.github.Jeff-Gillot:time-extension:1.0.0")
        testImplementation("io.strikt:strikt-core:0.31.0")
    }
}
