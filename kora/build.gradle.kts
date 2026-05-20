plugins {
    kotlin("jvm")
    kotlin("kapt")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    api(project(":core"))

    compileOnly(libs.kora.common)
    compileOnly(libs.kora.config.common)
    compileOnly(libs.kora.application.graph)
    compileOnly(libs.kora.database.jdbc)
    compileOnly(libs.kora.database.cassandra)
    compileOnly(libs.kora.kafka)
    compileOnly(libs.kora.http.client)
    compileOnly(libs.logback.classic)

    kapt(libs.kora.annotation.processor)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.assertj.core)

    testImplementation(libs.kora.common)
    testImplementation(libs.kora.config.common)
    testImplementation(libs.kora.config.hocon)
    testImplementation(libs.kora.application.graph)
    testImplementation(libs.kora.database.jdbc)
    testImplementation(libs.kora.database.cassandra)
    testImplementation(libs.kora.kafka)
    testImplementation(libs.kora.http.client)
    testImplementation(libs.kora.test.junit5)

    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.cassandra)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly(libs.postgresql.driver)
}

tasks.test {
    useJUnitPlatform()
    // Testcontainers on Docker Desktop 29 requires explicit API version override — default v1.32 is rejected by the server.
    systemProperty("api.version", "1.44")
    environment("DOCKER_API_VERSION", System.getenv("DOCKER_API_VERSION") ?: "1.44")
    listOf("DOCKER_HOST", "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "TESTCONTAINERS_RYUK_DISABLED").forEach { key ->
        System.getenv(key)?.let { environment(key, it) }
    }
}
