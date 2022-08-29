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
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Versioned - Persist - Version Store"

dependencies {
  implementation(platform(rootProject))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-spi"))
  implementation("com.google.protobuf:protobuf-java")
  implementation("com.google.code.findbugs:jsr305")
  implementation("com.google.guava:guava")
  implementation("org.slf4j:slf4j-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
}
