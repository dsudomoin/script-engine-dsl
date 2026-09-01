plugins {
    kotlin("jvm")
    alias(libs.plugins.ksp)
    `maven-publish`
}

kotlin {
    jvmToolchain(21)
}

base { archivesName.set("migration-dsl-kora") }

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

    // KSP, а не kapt: для Kotlin это единственный поддерживаемый Kora путь кодогенерации.
    ksp(libs.kora.symbol.processor)

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

    // Интеграционный тест собирает НАСТОЯЩИЙ @KoraApp-граф — ему нужен тот же процессор.
    kspTest(libs.kora.symbol.processor)

    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.cassandra)
    testImplementation(libs.testcontainers.kafka)
    testRuntimeOnly(libs.postgresql.driver)
}

tasks.test {
    // Контейнерные тесты требуют Docker и по умолчанию исключены: без этого обычный
    // `./gradlew build` невозможно прогнать на машине без Docker, и вместе с контейнерными
    // отваливались бы все остальные проверки модуля. Включить: `./gradlew test -PwithDocker`.
    val withDocker = providers.gradleProperty("withDocker").isPresent
    useJUnitPlatform {
        if (!withDocker) excludeTags("docker")
    }
    // Testcontainers on Docker Desktop 29 requires explicit API version override — default v1.32 is rejected by the server.
    systemProperty("api.version", "1.44")
    environment("DOCKER_API_VERSION", System.getenv("DOCKER_API_VERSION") ?: "1.44")
    listOf("DOCKER_HOST", "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "TESTCONTAINERS_RYUK_DISABLED").forEach { key ->
        System.getenv(key)?.let { environment(key, it) }
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "migration-dsl-kora"
            from(components["java"])
        }
    }
}
