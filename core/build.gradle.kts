plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    api(libs.kotlin.stdlib)
    api(libs.slf4j.api)
    implementation(libs.jackson.csv)
    implementation(libs.jackson.kotlin)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
    testImplementation(libs.assertj.core)
}

tasks.test { useJUnitPlatform() }
