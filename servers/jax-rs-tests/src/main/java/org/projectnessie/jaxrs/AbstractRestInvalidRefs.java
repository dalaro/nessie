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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestInvalidRefs extends AbstractRestEntries {
  @Test
  public void testInvalidNamedRefs() {
    ContentKey key = ContentKey.of("x");
    String invalidRef = "1234567890123456";

    assertThatThrownBy(() -> getApi().getCommitLog().refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(REF_NAME_MESSAGE);

    assertThatThrownBy(() -> getApi().getEntries().refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(REF_NAME_MESSAGE);

    assertThatThrownBy(() -> getApi().getContent().key(key).refName(invalidRef).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(REF_NAME_MESSAGE);

    assertThatThrownBy(() -> getApi().getContent().refName(invalidRef).key(key).get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageContaining("Bad Request (HTTP/400):")
        .hasMessageContaining(REF_NAME_MESSAGE);
  }

  @Test
  public void testValidHashesOnValidNamedRefs() throws BaseNessieClientServerException {
    Branch branch = createBranch("testValidHashesOnValidNamedRefs");

    int commits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, commits, currentHash);
    LogResponse entireLog = getApi().getCommitLog().refName(branch.getName()).get();
    assertThat(entireLog).isNotNull();
    assertThat(entireLog.getLogEntries()).hasSize(commits);

    EntriesResponse allEntries = getApi().getEntries().refName(branch.getName()).get();
    assertThat(allEntries).isNotNull();
    assertThat(allEntries.getEntries()).hasSize(commits);

    List<ContentKey> keys = new ArrayList<>();
    IntStream.range(0, commits).forEach(i -> keys.add(ContentKey.of("table" + i)));

    // TODO: check where hashOnRef is set
    Map<ContentKey, Content> allContent =
        getApi().getContent().keys(keys).refName(branch.getName()).get();

    for (int i = 0; i < commits; i++) {
      String hash = entireLog.getLogEntries().get(i).getCommitMeta().getHash();
      LogResponse log = getApi().getCommitLog().refName(branch.getName()).hashOnRef(hash).get();
      assertThat(log).isNotNull();
      assertThat(log.getLogEntries()).hasSize(commits - i);
      assertThat(ImmutableList.copyOf(entireLog.getLogEntries()).subList(i, commits))
          .containsExactlyElementsOf(log.getLogEntries());

      EntriesResponse entries =
          getApi().getEntries().refName(branch.getName()).hashOnRef(hash).get();
      assertThat(entries).isNotNull();
      assertThat(entries.getEntries()).hasSize(commits - i);

      int idx = commits - 1 - i;
      ContentKey key = ContentKey.of("table" + idx);
      Content c =
          getApi().getContent().key(key).refName(branch.getName()).hashOnRef(hash).get().get(key);
      assertThat(c).isNotNull().isEqualTo(allContent.get(key));
    }
  }

  @Test
  public void testUnknownHashesOnValidNamedRefs() throws BaseNessieClientServerException {
    Branch branch = createBranch("testUnknownHashesOnValidNamedRefs");
    String invalidHash = "1234567890123456";

    int commits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, commits, currentHash);
    assertThatThrownBy(
            () -> getApi().getCommitLog().refName(branch.getName()).hashOnRef(invalidHash).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () -> getApi().getEntries().refName(branch.getName()).hashOnRef(invalidHash).get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () ->
                getApi()
                    .getContent()
                    .key(ContentKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));

    assertThatThrownBy(
            () ->
                getApi()
                    .getContent()
                    .key(ContentKey.of("table0"))
                    .refName(branch.getName())
                    .hashOnRef(invalidHash)
                    .get())
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(
            String.format(
                "Could not find commit '%s' in reference '%s'.", invalidHash, branch.getName()));
  }

  @Test
  public void invalidPutViaAPI() throws BaseNessieClientServerException
  {
    Branch branch = createBranch("invalidPut");

    ContentKey contentKey = ContentKey.of("foo");
    IcebergTable contentToWrite =
      IcebergTable.of("/iceberg/table", 42, 42, 42, 42);
    IcebergTable expectedContent =
      IcebergTable.of("/iceberg/table", 777, 777, 777, 777);
    Operation.Put putExistingRef = Operation.Put.of(contentKey, contentToWrite, expectedContent);

    Operation.Put putNewRef = Operation.Put.of(contentKey, expectedContent);

    // 1) every Put operation for a new content key must have an empty/no expectedContent
    assertThatThrownBy( () ->
      getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(putExistingRef)
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit())
      .hasMessageStartingWith("Bad Request (HTTP/400): Expected content must not be set when creating new content");


    getApi()
      .commitMultipleOperations()
      .branch(branch)
      .operation(putNewRef)
      .commitMeta(CommitMeta.fromMessage("commit 1"))
      .commit();

    // TODO get the reference we just put, and read its server-assigned content id (really UUID.random)
    Map<ContentKey, Content> contents = getApi()
      .getContent()
      .key(contentKey)
      .refName(branch.getName())
      .get();

    // TODO rely on AbstractMapAssert via assertj statics instead
    assertNotNull(contents);
    assertEquals(1, contents.size());
    Map.Entry<ContentKey, Content> ent = contents.entrySet().iterator().next();
    String assignedContentId = ent.getValue().getId();

    contentToWrite = IcebergTable.of("/iceberg/table", 42, 42, 42, 42, assignedContentId);

    expectedContent =
      IcebergTable.of("/iceberg/table", 777, 777, 777, 777, assignedContentId);
    Operation.Put putExistingRef2 = Operation.Put.of(contentKey, contentToWrite, expectedContent);

    Reference refBranch2 = getApi().getReference().refName(branch.getName()).get();
    assertEquals(Reference.ReferenceType.BRANCH, refBranch2.getType());
    Branch branch2 = Branch.of(branch.getName(), refBranch2.getHash());

    // 2) every Put operation for an existing content key must have a non-empty expectedContent
    getApi()
      .commitMultipleOperations()
      .branch(branch2)
      .operation(putExistingRef2)
      .commitMeta(CommitMeta.fromMessage("commit 2"))
      .commit();

    Reference refBranch3 = getApi().getReference().refName(branch.getName()).get();
    assertEquals(Reference.ReferenceType.BRANCH, refBranch3.getType());
    Branch branch3 = Branch.of(branch.getName(), refBranch3.getHash());

    contentToWrite = IcebergTable.of("/iceberg/table", 42, 42, 42, 42, assignedContentId);
    expectedContent =
      IcebergTable.of("/iceberg/table", 777, 777, 777, 777, "blah blah blah");
    Operation.Put putExistingRef3 = Operation.Put.of(contentKey, contentToWrite, expectedContent);

    // 3) verify that the content id of the content in a Put operation is equal to the content id of a non-empty expectedContent
    // (for an existing ref, request-supplied expected contentid == existing contentid =? request-supplied to-write contentid)
    assertThatThrownBy( () ->
      getApi()
        .commitMultipleOperations()
        .branch(branch3)
        .operation(putExistingRef3)
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit()).hasMessageContaining("content differ for key 'foo'");

    contentToWrite = IcebergTable.of("/iceberg/table", 42, 42, 42, 42, "blah blah blah");
    expectedContent =
      IcebergTable.of("/iceberg/table", 777, 777, 777, 777, "blah blah blah");
    Operation.Put putExistingRef4 = Operation.Put.of(contentKey, contentToWrite, expectedContent);


    getApi()
      .commitMultipleOperations()
      .branch(branch3)
      .operation(putExistingRef4)
      .commitMeta(CommitMeta.fromMessage("commit 1"))
      .commit();


    contentToWrite = IcebergTable.of("/iceberg/table", 42, 42, 42, 42, "blah blah blah");
    expectedContent =
      IcebergTable.of("/iceberg/table", 777, 777, 777, 777, assignedContentId);
    Operation.Put putExistingRef5 = Operation.Put.of(contentKey, contentToWrite, expectedContent);


    assertThatThrownBy( () ->
      getApi()
        .commitMultipleOperations()
        .branch(branch3)
        .operation(putExistingRef5)
        .commitMeta(CommitMeta.fromMessage("commit 1"))
        .commit()).hasMessageContaining("content differ for key 'foo'");

  }
}
