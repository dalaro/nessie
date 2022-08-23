/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.perftest.gatling

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import org.projectnessie.error.NessieReferenceNotFoundException
import org.projectnessie.model.Operation.Put
import org.projectnessie.model._
import org.projectnessie.perftest.gatling.Predef.nessie

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{FiniteDuration, HOURS, NANOSECONDS, SECONDS}
import scala.util.Random
import scala.jdk.CollectionConverters._

/** Gatling simulation to perform commits against Nessie. It has a bunch of
  * configurables, see [[CommitToBranchParams]]
  */
class KeyListSpillingSimulation extends Simulation {


  val paddingBuilder = new StringBuilder()

  for (i <- 1 to 32) {
    paddingBuilder.append("abcdefg_")
  }
  val padding = paddingBuilder.toString();

  val params: KeyListSpillingParams = KeyListSpillingParams.fromSystemProperties()

  /** The actual benchmark code to measure Nessie-commit performance in various
    * scenarios.
    */
  private def commitToBranch: ChainBuilder = {
    val chain = {
      /*
      exec(
        // fetch the previous state of the table (if exists)
        nessie("FetchContent")
          .execute { (client, session) =>
            // Current Nessie Branch object
            val branch = session("branch").as[Branch]
            // Table used in the Nessie commit
            val tableName = params.makeTableName(session)

            val key = ContentKey.of("name", "space", tableName)

            val existingTable =
              client.getContent.reference(branch).key(key).get().get(key)

            // Store precomputed values in the session
            session
              .set("existingTable", Option(existingTable))
              .set("key", key)
          }
      ).
      */
      exec(
        // Create / update the table
        nessie("Commit")
          .execute { (client, session) =>

            val parentHash = client.getReference.refName(params.getBranchName()).get()

            val commitIndex : Int =
              if (session.contains("commitIndex")) {
                session("commitIndex").as[Int]
              } else {
                new Random().nextInt()
              }

            // The commit number is the loop-variable declared buildScenario()
            val commitNum = session("commitNum").asOption[Int].get
            // Current Nessie Branch object
            val branch = session("branch").as[Branch]
//            val branch = Branch.of(params.getBranchName(), parentHash.getHash)
            // Our "user ID", an integer supplied by Gatling
            val userId = session.userId
            // Precomputed key and table from the previous action
//            val key = session("key").as[ContentKey]
//            val key = ContentKey.of(s"synthetic-key worker=$userId commitIndex=$commitIndex")
            val key = ContentKey.of(s"$userId $commitIndex" + padding)

            val expectedTable = Option.empty

            val metadataLocation = s"metadata_${userId}_$commitNum"

            val table =
              if (expectedTable.isEmpty)
                IcebergTable.of(metadataLocation, 42, 43, 44, 45)
              else
                ImmutableIcebergTable.builder
                  .from(expectedTable.get)
                  .metadataLocation(metadataLocation)
                  .build()

            // Call the Nessie client operation to perform a commit
            val updatedBranch = client
              .commitMultipleOperations()
              .branch(branch)
              .commitMeta(
                CommitMeta.fromMessage(s"worker commit userId=$userId commitNum=$commitNum")
              )
              .operation(Put.of(key, table, expectedTable.orNull))
              .commit()

            session.set("branch", updatedBranch).set("commitIndex", commitIndex + 1)
          }
      )
    }

    if (params.opRate > 0) {
      // "pace" the commits, if commit-rate is configured
      val oneHour = FiniteDuration(1, HOURS)
      val nanosPerIteration =
        oneHour.toNanos / (params.opRate * oneHour.toSeconds)
      pace(FiniteDuration(nanosPerIteration.toLong, NANOSECONDS))
        .exitBlockOnFail(chain)
    } else {
      // if no commit-rate is configured, run "as fast as possible"
      chain
    }
  }

  /** Get the [[Branch]] object, create the branch in Nessie if needed.
    */
  private def getReference: ChainBuilder = {
    // If we don't have a reference for the branch yet, then try to create the branch and try to fetch the reference
    exec(
      nessie(s"Create branch $params.branch")
        .execute { (client, session) =>
          val parentBranch: Reference = client
            .getReference
            .refName(params.getBranchName())
            .get()

          // create the branch (errors will be ignored)
          val branch = client
            .createReference()
            .sourceRefName(params.getBranchName())
            .reference(Branch.of(params.makeBranchName(session), parentBranch.getHash))
            .create()
            .asInstanceOf[Branch]
          session.set("branch", parentBranch)
        }
        // ignore any exception, handled in the following `doIf()`
        .ignoreException()
        // don't measure/log this action
        .dontLog()
    ).doIf(session => !session.contains("branch")) {
      exec(
        nessie(s"Get reference $params.branch")
          .execute { (client, session) =>
            // retrieve the Nessie branch reference and store it in the Gatling session object
            val branch = client.getReference
              .refName(params.makeBranchName(session))
              .get()
              .asInstanceOf[Branch]

            val parentBranch: Reference = client
              .getReference
              .refName(params.getBranchName())
              .get()
            session.set("branch", parentBranch)
          }
          // don't measure/log this action
          .dontLog()
      )
    }
  }

  private def buildScenario(): ScenarioBuilder = {
    val scn = scenario("KeyList-Spilling")
      .exec(getReference)

    if (params.numberOfCommits > 0) {
      // Process configured number of commits
      scn.repeat(params.numberOfCommits, "commitNum") {
        commitToBranch
      }
    } else {
      // otherwise run "forever" (or until "max-duration")
      scn.forever("commitNum") {
        commitToBranch
      }
    }
  }

  /** Sets up the simulation. Implemented as a function to respect the optional
    * maximum-duration.
    */
  private def doSetUp(): SetUp = {
    val nessieProtocol: NessieProtocol = nessie().clientFromSystemProperties()

    val branchName = params.getBranchName();

    // Drop test branch, if it exists
    try {
      val ref = nessieProtocol.client.getReference.refName(branchName).get()

      if (null != ref) {
        nessieProtocol.client.deleteBranch().branch(Branch.of(params.getBranchName(), ref.getHash)).delete()
      }
    } catch {
      case e: NessieReferenceNotFoundException => {
        println(s"Reference $branchName does not exist, not attempting to delete")
      }
    }

    // Create test branch
    nessieProtocol.client.createReference().reference(Branch.of(params.getBranchName(), null)).create()

    val branch = nessieProtocol.client.getReference.refName(branchName).get().asInstanceOf[Branch]

    // Load test fixture
    loadData(nessieProtocol, branch)

    System.out.println(params.asPrintableString())

    // Handoff read phase to Gatling
//    var s: SetUp = setUp(buildScenario().inject(atOnceUsers(params.numUsers)))
    var s: SetUp = setUp(buildScenario().inject(
      constantConcurrentUsers(1/* params.numUsers */).during(scala.concurrent.duration.Duration.apply(1, TimeUnit.SECONDS))
    ))
    if (params.durationSeconds > 0) {
      s = s.maxDuration(FiniteDuration(params.durationSeconds, SECONDS))
    }
    s.protocols(nessieProtocol)
  }

  private def loadData(nessieProtocol: NessieProtocol, branch: Branch) = {

    val commitCount = 98
    val operationCount = 10000

    for (i <- 1 to commitCount) {

      val operations: List[Operation] = LazyList.from(i * operationCount)
        .take(operationCount)
        .map(commitNum => String.valueOf(commitNum) + " " + padding)
        .map(keyName => Operation.Put.of(
          ContentKey.of(keyName),
          IcebergTable.of("/iceberg/table", 42, 42, 42, 42)))
        .toList

      println("op count: " + operations.size)

      nessieProtocol.client.commitMultipleOperations()
        .operations(operations.asJava)
        .branch(branch)
        .commitMeta(CommitMeta.fromMessage(s"commit $i / $commitCount"))
        .commit();
    }
  }

  // This is where everything starts, doSetUp() returns the `SetUp` ...

  doSetUp()
}
