/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
  `java-library`
  jacoco
  `maven-publish`
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Versioned - Persist - Adapter"

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))

  implementation(project(":nessie-versioned-spi"))
  compileOnly("org.immutables:value-annotations")
  compileOnly("org.immutables:value-fixture")
  annotationProcessor("org.immutables:value-processor")
  implementation("com.google.protobuf:protobuf-java")
  implementation("com.google.code.findbugs:jsr305")
  implementation("com.google.guava:guava")
  implementation("org.slf4j:slf4j-api")
  implementation("org.agrona:agrona")

  implementation(platform("io.quarkus:quarkus-bom"))
  implementation("io.opentracing:opentracing-api")
  implementation("io.opentracing:opentracing-util")
  implementation("io.micrometer:micrometer-core")

  testImplementation(platform(rootProject))

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.mockito:mockito-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
