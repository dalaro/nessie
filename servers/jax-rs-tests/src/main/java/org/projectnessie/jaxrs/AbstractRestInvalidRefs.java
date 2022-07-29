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
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
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
   * @see AbstractRestInvalidWithHttp#invalidPutViaHTTP(Content.Type)
   */
  @ParameterizedTest(name = "invalidPutViaAPI [{index}] {arguments}")
  @EnumSource(value = Content.Type.class, mode = EnumSource.Mode.EXCLUDE, names = {"UNKNOWN", "NAMESPACE"})
  public void invalidPutViaAPI(Content.Type contentType) throws BaseNessieClientServerException {
    final String branchName = "invalidPutViaAPI";
    final ContentKey contentKey = ContentKey.of("foo");
    final String fakeId = "bar";
    Branch branch = createBranch(branchName);

    // Specifying expected/existing content when creating new content should fail
    assertThatThrownBy(
            () ->
                getApi()
                    .commitMultipleOperations()
                    .branch(getBranch(branchName))
                    .operation(Put.of(contentKey, makeContentWithId(contentType, null), makeContentWithId(contentType,null)))
                    .commitMeta(CommitMeta.fromMessage("failed initial put"))
                    .commit())
        .hasMessageStartingWith(
            "Bad Request (HTTP/400): Expected content must not be set when creating new content");

    // TODO either (a) delete or (b) uncomment and correct the message assertion, pending discussion
    // Specifying a non-null content ID when creating new content should fail
//    assertThatThrownBy(
//      () ->
//        getApi()
//          .commitMultipleOperations()
//          .branch(branch)
//          .operation(Put.of(contentKey, getIcebergTable("bar")))
//          .commitMeta(CommitMeta.fromMessage("failed initial put"))
//          .commit())
//      .hasMessageStartingWith(
//        "Bad Request (HTTP/400): TODO write exception message");

    // Contrasting previous intentional failure, create content successfully
    getApi()
        .commitMultipleOperations()
        .branch(branch)
        .operation(Put.of(contentKey, makeContentWithId(contentType,null)))
        .commitMeta(CommitMeta.fromMessage("successful initial put"))
        .commit();

    // Read the content we created and record the content ID
    Map<ContentKey, Content> contents =
        getApi().getContent().key(contentKey).refName(branch.getName()).get();
    assertThat(contents).isNotNull().hasSize(1).containsOnlyKeys(contentKey);
    String assignedContentId = Iterables.getOnlyElement(contents.values()).getId();

    Put putWithMatchingContentIds =
        Put.of(
            contentKey, makeContentWithId(contentType, assignedContentId), makeContentWithId(contentType, assignedContentId));

    // Attempt to overwrite content as if it was still new, which should fail now
    assertThatThrownBy(
      () ->
    getApi()
      .commitMultipleOperations()
      .branch(getBranch(branchName))
      .operation(Put.of(contentKey, makeContentWithId(contentType,null)))
      .commitMeta(CommitMeta.fromMessage("should fail"))
      .commit())
      .hasMessageStartingWith(
        "Bad Request (HTTP/400): Existing content found with content-id '%s' for key '%s'",
        assignedContentId, contentKey.toPathString());

    // Overwrite content, specifying matching content IDs on expected and new content
    getApi()
        .commitMultipleOperations()
        .branch(getBranch(branchName))
        .operation(putWithMatchingContentIds)
        .commitMeta(CommitMeta.fromMessage("successful overwrite put"))
        .commit();

    // Attempt overwrite, but specify a bogus content ID in the expected content
    Put putWithInvalidExpectedContentId =
      Put.of(contentKey, makeContentWithId(contentType, assignedContentId), makeContentWithId(contentType, fakeId));
    assertThatThrownBy(
            () ->
                getApi()
                    .commitMultipleOperations()
                    .branch(getBranch(branchName))
                    .operation(putWithInvalidExpectedContentId)
                    .commitMeta(CommitMeta.fromMessage("put with invalid expected content id"))
                    .commit())
        .hasMessage("Bad Request (HTTP/400): Content ids for new ('%s') and expected ('%s') content differ for key '%s'",
          assignedContentId, fakeId, contentKey);

    // Attempt similar overwrite as immediately above, but specify a bogus content ID on the new content instead
    Put putWithIncorrectNewContentId =
      Put.of(contentKey, makeContentWithId(contentType, fakeId), makeContentWithId(contentType, assignedContentId));
    assertThatThrownBy(
      () ->
        getApi()
          .commitMultipleOperations()
          .branch(getBranch(branchName))
          .operation(putWithIncorrectNewContentId)
          .commitMeta(CommitMeta.fromMessage("put with incorrect new content id"))
          .commit())
      .hasMessageContaining("Bad Request (HTTP/400): Content ids for new ('%s') and expected ('%s') content differ for key '%s'",
        fakeId, assignedContentId, contentKey);

    // Attempt overwrite, specifying new and expected content IDs which match each other, but not existing data
    Put putWithMatchingFakeContentIds =
        Put.of(contentKey, makeContentWithId(contentType, fakeId), makeContentWithId(contentType, fakeId));
    assertThatThrownBy(
      () ->
    getApi()
        .commitMultipleOperations()
        .branch(getBranch(branchName))
        .operation(putWithMatchingFakeContentIds)
        .commitMeta(CommitMeta.fromMessage("put with matching fake content ids"))
        .commit())
      .hasMessageContaining("Expected content-id '%s' conflicts with actual content-id", fakeId);
  }

  private static final ImmutableMap<Content.Type, Function<String, Content>> TEST_CONTENT_FACTORIES =
    ImmutableMap.of(
      Content.Type.DELTA_LAKE_TABLE, id ->
        ImmutableDeltaLakeTable.builder().id(id)
          .addMetadataLocationHistory("/delta/metadata")
          .addCheckpointLocationHistory("/delta/checkpoint")
          .build(),
      Content.Type.ICEBERG_TABLE, id ->
        ImmutableIcebergTable.of("/iceberg/table", 42, 42, 42, 42, id),
      Content.Type.ICEBERG_VIEW, id ->
        ImmutableIcebergView.of(id, "/iceberg/view", 42, 42, "dialect", "statement")
    );

  protected static Content makeContentWithId(final Content.Type type, final String id) {
    Function<String, Content> factory = TEST_CONTENT_FACTORIES.get(type);
    if (null == factory) {
      throw new IllegalArgumentException("Unsupported content type: " + type);
    }
    return factory.apply(id);
  }

  private Branch getBranch(final String branchName) throws NessieNotFoundException {
    Reference ref = getApi().getReference().refName(branchName).get();
    assertEquals(Reference.ReferenceType.BRANCH, ref.getType());
    return Branch.of(branchName, ref.getHash());
  }
}
