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
    compileOnly(libs.logback.classic)

    // KSP, а не kapt: для Kotlin это единственный поддерживаемый Kora путь кодогенерации.
    ksp(libs.kora.symbol.processor)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.assertj.core)

    testImplementation(libs.kora.common)
    testImplementation(libs.kora.config.common)
    testImplementation(libs.kora.application.graph)
}

tasks.test { useJUnitPlatform() }

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "migration-dsl-kora"
            from(components["java"])
        }
    }
}
