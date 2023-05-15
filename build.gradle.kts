plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}
var calciteVersion = "1.32.0"
var testContainerVersion = "1.17.5"
var lombokDependency = "org.projectlombok:lombok:1.18.22"

dependencies {
    compileOnly(lombokDependency)
    annotationProcessor(lombokDependency)
    implementation("org.apache.calcite:calcite-core:${calciteVersion}")
    implementation("org.apache.calcite:calcite-csv:${calciteVersion}")
    implementation("org.postgresql:postgresql:42.5.0")
    implementation("mysql:mysql-connector-java:8.0.33")
    implementation("org.jooq:jooq:3.16.12")
    implementation("commons-io:commons-io:2.11.0")

    testCompileOnly(lombokDependency)
    testAnnotationProcessor(lombokDependency)
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:${testContainerVersion}")
    testImplementation("org.testcontainers:junit-jupiter:${testContainerVersion}")
}

tasks.test {
    useJUnitPlatform()
}