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

  /**
   * @see AbstractRestInvalidWithHttp#invalidPutViaHttp()
   */
  @Test
  public void invalidPutViaAPI() throws BaseNessieClientServerException {
    final String branchName = "invalidPutViaAPI";
    Branch branch = createBranch(branchName);
    ContentKey contentKey = ContentKey.of("foo");

    Operation.Put putWithExpected =
        Operation.Put.of(contentKey, getIcebergTable(null), getIcebergTable(null));
    // 1) every Put operation for a new content key must have an empty/no expectedContent
    assertThatThrownBy(
            () ->
                getApi()
                    .commitMultipleOperations()
                    .branch(branch)
                    .operation(putWithExpected)
                    .commitMeta(CommitMeta.fromMessage("failed initial put"))
                    .commit())
        .hasMessageStartingWith(
            "Bad Request (HTTP/400): Expected content must not be set when creating new content");

    Operation.Put putWithoutExpected = Operation.Put.of(contentKey, getIcebergTable(null));
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(putWithoutExpected)
        .commitMeta(CommitMeta.fromMessage("successful initial put"))
        .commit();

    Map<ContentKey, Content> contents =
        getApi().getContent().key(contentKey).refName(branch.getName()).get();
    // TODO rely on AbstractMapAssert via assertj statics instead
    assertNotNull(contents);
    assertEquals(1, contents.size());
    Map.Entry<ContentKey, Content> ent = contents.entrySet().iterator().next();
    String assignedContentId = ent.getValue().getId();

    Operation.Put putWithMatchingContentIds =
        Operation.Put.of(
            contentKey, getIcebergTable(assignedContentId), getIcebergTable(assignedContentId));

    // 2) every Put operation for an existing content key must have a non-empty expectedContent
    getApi()
        .commitMultipleOperations()
        .branch(getBranch(branchName))
        .operation(putWithMatchingContentIds)
        .commitMeta(CommitMeta.fromMessage("successful overwrite put"))
        .commit();

    Operation.Put putWithInvalidExpectedContentId =
        Operation.Put.of(contentKey, getIcebergTable(assignedContentId), getIcebergTable("foobar"));
    // 3) verify that the content id of the content in a Put operation is equal to the content id of
    // a non-empty expectedContent
    // (for an existing ref, request-supplied expected contentid == existing contentid =?
    // request-supplied to-write contentid)
    assertThatThrownBy(
            () ->
                getApi()
                    .commitMultipleOperations()
                    .branch(getBranch(branchName))
                    .operation(putWithInvalidExpectedContentId)
                    .commitMeta(CommitMeta.fromMessage("put with invalid expected content id"))
                    .commit())
        .hasMessageContaining("content differ for key 'foo'");

    String fakeId = "foobar";
    Operation.Put putWithMatchingFakeContentIds =
        Operation.Put.of(contentKey, getIcebergTable(fakeId), getIcebergTable(fakeId));
    assertThatThrownBy(
      () ->
    getApi()
        .commitMultipleOperations()
        .branch(getBranch(branchName))
        .operation(putWithMatchingFakeContentIds)
        .commitMeta(CommitMeta.fromMessage("put with matching fake content ids"))
        .commit())
      .hasMessageContaining("Conflict between expected content-id '%s' and actual content-id", fakeId);

    Operation.Put putWithIncorrectNewContentId =
        Operation.Put.of(contentKey, getIcebergTable("foobar"), getIcebergTable(assignedContentId));
    assertThatThrownBy(
            () ->
                getApi()
                    .commitMultipleOperations()
                    .branch(getBranch(branchName))
                    .operation(putWithIncorrectNewContentId)
                    .commitMeta(CommitMeta.fromMessage("put with incorrect new content id"))
                    .commit())
        .hasMessageContaining("content differ for key 'foo'");
  }

  protected static IcebergTable getIcebergTable(String contentId) {
    return IcebergTable.of("/iceberg/table", 42, 42, 42, 42, contentId);
  }

  private Branch getBranch(final String branchName) throws NessieNotFoundException {
    Reference ref = getApi().getReference().refName(branchName).get();
    assertEquals(Reference.ReferenceType.BRANCH, ref.getType());
    return Branch.of(branchName, ref.getHash());
  }
}
