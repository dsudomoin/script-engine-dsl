// Модуль-потребитель: собирается ровно так, как собирался бы чужой сервис, подключивший
// библиотеку. Здесь и живёт проверка того, что документированный wire-up действительно
// работает — внутри :kora такую проверку не поставить, KSP-расширение конфига Kora не может
// сгенерировать экстрактор в том же модуле, где он уже сгенерирован для main.
plugins {
    kotlin("jvm")
    alias(libs.plugins.ksp)
    application
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    implementation(project(":kora"))

    implementation(libs.kora.common)
    implementation(libs.kora.config.common)
    implementation(libs.kora.config.hocon)
    implementation(libs.kora.application.graph)
    runtimeOnly(libs.logback.classic)

    ksp(libs.kora.symbol.processor)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    kspTest(libs.kora.symbol.processor)
}

application {
    mainClass.set("io.github.dsudomoin.migration.example.ExampleAppKt")
}

tasks.test { useJUnitPlatform() }
