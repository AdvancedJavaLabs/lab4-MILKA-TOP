plugins {
    id("java")
}

group = "ru.itmo"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")
    implementation("org.apache.hadoop:hadoop-common:3.4.1")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}