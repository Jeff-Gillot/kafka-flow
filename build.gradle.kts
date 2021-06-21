plugins {
    kotlin("jvm") version "1.5.10"
}

group = "org.example"
version = "0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

subprojects {
    apply(plugin = "kotlin")
    repositories {
        mavenCentral()
    }

    kotlin {
        explicitApi()
    }

    dependencies {
        implementation(kotlin("stdlib"))
    }
}

val kafkaVersion = "2.8.0"
//project(":kafka-flow-topic-descriptor") {
//    dependencies {
//        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
//    }
//}
//
//project(":kafka-flow-processor") {
//    dependencies {
//        implementation(project(":kafka-flow-topic-descriptor"))
//        implementation(project(":kafka-flow-client"))
//        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
//        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
//    }
//}
//
project(":kafka-flow-client") {
    dependencies {
//        implementation(project(":kafka-flow-topic-descriptor"))
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    }
}

//project(":kafka-flow-testing") {
//    dependencies {
//        implementation(project(":kafka-flow-topic-descriptor"))
//        implementation(project(":kafka-flow-client"))
//        implementation(project(":kafka-flow-processor"))
//        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.0")
//        implementation("org.apache.kafka:kafka_2.13:$kafkaVersion")
//        implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
//        implementation("io.github.microutils:kotlin-logging-jvm:2.0.6")
//        implementation("org.slf4j:slf4j-simple:1.7.29")
//        implementation("org.apache.zookeeper:zookeeper:3.7.0")
//        implementation("io.dropwizard.metrics:metrics-core:4.1.20")
//        implementation("org.awaitility:awaitility:4.0.3")
//    }
//}
