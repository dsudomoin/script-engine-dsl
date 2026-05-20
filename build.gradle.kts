plugins {
    kotlin("jvm") version "2.1.20" apply false
    kotlin("kapt") version "2.1.20" apply false
}

subprojects {
    group = "io.github.dsudomoin.migration"
    version = "0.1.0"

    repositories { mavenCentral() }
}
