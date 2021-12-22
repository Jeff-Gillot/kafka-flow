import java.net.URI

plugins {
    kotlin("jvm") version "1.6.10"
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
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
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
                version = "1.2.6-SNAPSHOT"

                from(components["java"])
            }
        }
    }
}

val kafkaVersion = "2.8.0"
project(":kafka-flow-topic-descriptor") {
    dependencies {
        api("com.sangupta:murmur:1.0.0")
    }
}

project(":kafka-flow-client") {
    dependencies {
        api(project(":kafka-flow-topic-descriptor"))
        api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
        api("org.apache.kafka:kafka-clients:$kafkaVersion")
        api( "com.github.Jeff-Gillot:time-extension:1.0.0")

        testImplementation("io.strikt:strikt-core:0.31.0")
    }
}

project(":kafka-flow-metrics") {
    dependencies {
        api(project(":kafka-flow-client"))
        api(project(":kafka-flow-topic-descriptor"))

        implementation("io.dropwizard.metrics:metrics-core:4.2.4")
    }
}

project(":kafka-flow-testing") {
    dependencies {
        api(project(":kafka-flow-topic-descriptor"))
        api(project(":kafka-flow-client"))
        api("io.github.microutils:kotlin-logging-jvm:2.0.6")
        api("org.testcontainers:testcontainers:1.15.3")
        api("org.testcontainers:kafka:1.15.3")

        implementation("org.slf4j:slf4j-simple:1.7.29")

        testImplementation("io.strikt:strikt-core:0.31.0")
    }
}
