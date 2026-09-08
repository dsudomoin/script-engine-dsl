plugins {
    kotlin("jvm")
    `maven-publish`
}

kotlin {
    jvmToolchain(21)
}

// Имя артефакта публикации. Модуль Gradle называется `core`, но наружу он уезжает как
// `migration-dsl-core` — координаты из README/USER_GUIDE/AGENTS §3 должны быть настоящими.
base { archivesName.set("migration-dsl-core") }

dependencies {
    api(libs.kotlin.stdlib)
    api(libs.slf4j.api)
    implementation(libs.kotlin.reflect)
    implementation(libs.jackson.csv)
    implementation(libs.jackson.kotlin)
    implementation(libs.jackson.jsr310)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.assertj.core)
}

tasks.test { useJUnitPlatform() }

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "migration-dsl-core"
            from(components["java"])
        }
    }
}
