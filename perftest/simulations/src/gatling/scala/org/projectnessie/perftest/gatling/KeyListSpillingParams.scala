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

import io.gatling.core.session.Session

/** @param numberOfCommits
  *   number of commits that each simulated user creates (defaults to 100)
  * @param numUsers
  *   see [[BaseParams.numUsers]]
  * @param opRate
  *   see [[BaseParams.opRate]]
  * @param note
  *   see [[BaseParams.note]]
  */
case class KeyListSpillingParams(
                                  numberOfCommits: Int,
                                  putsPerCommit: Int,
                                  durationSeconds: Int,
                                  override val numUsers: Int,
                                  override val opRate: Double,
                                  override val note: String
) extends BaseParams {

  override def asPrintableString(): String = {
    s"""${super.asPrintableString().trim}
    |   num-commits:   $numberOfCommits
    |   puts-per-commit:   $putsPerCommit
    |   duration:       $durationSeconds
    |""".stripMargin
  }

  def getBranchName(): String = {
    "keylist_spilling_base"
  }

  def makeTableName(session: Session): String = {
    "keylist_spilling_t"
  }

  def makeBranchName(session: Session): String = {
    s"keylist_spilling_${session.userId}"
  }
}


object KeyListSpillingParams {
  def fromSystemProperties(): KeyListSpillingParams = {
    val base = BaseParams.fromSystemProperties()
    val numberOfCommits: Int = Integer.getInteger("sim.commits", 400).toInt
    val putsPerCommit: Int = Integer.getInteger("sim.putsPerCommit", 100).toInt
    val durationSeconds: Int =
      Integer.getInteger("sim.duration.seconds", 0).toInt
    KeyListSpillingParams(
      numberOfCommits,
      putsPerCommit,
      durationSeconds,
      base.numUsers,
      base.opRate,
      base.note
    )
  }
}
