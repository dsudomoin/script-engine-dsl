plugins {
    kotlin("jvm") version "2.1.20" apply false
    alias(libs.plugins.ksp) apply false
}

subprojects {
    group = "io.github.dsudomoin.migration"
    version = "0.2.0"

    repositories { mavenCentral() }
}
