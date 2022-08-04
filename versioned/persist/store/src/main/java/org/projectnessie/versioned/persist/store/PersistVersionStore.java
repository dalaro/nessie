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
package org.projectnessie.versioned.persist.store;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableRefLogDetails;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.MergeConflictException;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.RefLogDetails;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.TransplantParams;

public class PersistVersionStore<CONTENT, METADATA, CONTENT_TYPE extends Enum<CONTENT_TYPE>>
    implements VersionStore<CONTENT, METADATA, CONTENT_TYPE> {

  private final DatabaseAdapter databaseAdapter;
  protected final StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker;

  public PersistVersionStore(
      DatabaseAdapter databaseAdapter, StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker) {
    this.databaseAdapter = databaseAdapter;
    this.storeWorker = storeWorker;
  }

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return databaseAdapter.hashOnReference(namedReference, hashOnReference);
  }

  @Nonnull
  @Override
  public Hash noAncestorHash() {
    return databaseAdapter.noAncestorHash();
  }

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> expectedHead,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<CONTENT>> operations,
      @Nonnull Callable<Void> validator)
      throws ReferenceNotFoundException, ReferenceConflictException {

    ImmutableCommitParams.Builder commitAttempt =
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .expectedHead(expectedHead)
            .commitMetaSerialized(serializeMetadata(metadata))
            .validator(validator);

    Map<Key, ContentId> expectedContentIdByKey = new HashMap<>();

    for (Operation<CONTENT> operation : operations) {
      if (operation instanceof Put) {
        Put<CONTENT> op = (Put<CONTENT>) operation;
        CONTENT content = op.getValue();
        CONTENT expected = op.getExpectedValue();

        if (storeWorker.getId(content) == null) {
          // No content-ID --> New content

          Preconditions.checkArgument(
              expected == null,
              "Expected content must not be set when creating new content. "
                  + "The put operation's content has no content ID and is considered as new. "
                  + "Key: '%s'",
              op.getKey());

          // assign content-ID
          content = storeWorker.applyId(content, UUID.randomUUID().toString());
        }

        ContentId contentId = ContentId.of(storeWorker.getId(content));
        commitAttempt.addPuts(
            KeyWithBytes.of(
                op.getKey(),
                contentId,
                storeWorker.getPayload(content),
                storeWorker.toStoreOnReferenceState(content, commitAttempt::addAttachments)));

        if (expected != null) {
          String expectedId = storeWorker.getId(expected);
          Preconditions.checkArgument(
              expectedId != null,
              "Content id for expected content must not be null, key '%s'",
              op.getKey());
          ContentId expectedContentId = ContentId.of(expectedId);
          Preconditions.checkArgument(
              contentId.equals(expectedContentId),
              "Content ids for new ('%s') and expected ('%s') content differ for key '%s'",
              contentId,
              expectedContentId,
              op.getKey());
          // Retain this (content-key, expected-content-id) pair for conflict-check against existing committed data
          if (expectedHead.isPresent()) {
            expectedContentIdByKey.put(op.getKey(), expectedContentId);
          }
        }

        Preconditions.checkState(
            !storeWorker.requiresGlobalState(content),
            "Nessie no longer supports content with global state");
      } else if (operation instanceof Delete) {
        commitAttempt.addDeletes(operation.getKey());
      } else if (operation instanceof Unchanged) {
        commitAttempt.addUnchanged(operation.getKey());
      } else {
        throw new IllegalArgumentException(String.format("Unknown operation type '%s'", operation));
      }
    }

    /*
     * If expectedHead is present, then attempt to detect conflicts between:
     *
     * a) expected content-ids provided by Put operations, and
     *
     * b) actual content-ids present at the HEAD referenced by expectedHash (at the same content-keys)
     */
    if (expectedHead.isPresent()) {
      Callable<Void> putExpectationsValidator = () -> {
        // Retrieve keys where the key's actual content-id does not match a Put operation's expected content-id
        KeyFilterPredicate brokenPutExpectationsPredicate = (key, contentId, type) -> {
          ContentId expectedContentId = expectedContentIdByKey.get(key);
          return null != expectedContentId && !contentId.equals(expectedContentId);
        };
        Stream<KeyListEntry> conflictEntries = databaseAdapter.keys(expectedHead.get(), brokenPutExpectationsPredicate);

        // TODO consider logging additional conflicts, if present, or perhaps adding a count of suppressed conflicts
        Optional<KeyListEntry> conflictEntry = conflictEntries.findFirst();
        if (conflictEntry.isPresent()) {
          KeyListEntry kle = conflictEntry.get();
          ContentId existing = kle.getContentId();
          Key key = kle.getKey();
          ContentId expected = expectedContentIdByKey.get(key);
          Preconditions.checkState(null != expected, String.format("Null expected content-id for key '%s'", key));
          throw new IllegalArgumentException(String.format("Conflict between expected content-id '%s' and actual content-id '%s' for key '%s'",
            expected, existing, key));
        }

        return null;
      };

      Callable<Void> combinedValidator = () -> { validator.call(); putExpectationsValidator.call(); return null; };
      commitAttempt.validator(combinedValidator);
    }

    return databaseAdapter.commit(commitAttempt.build());
  }

  @Override
  public MergeResult<Commit<METADATA, CONTENT>> transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant,
      MetadataRewriter<METADATA> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<Key, MergeType> mergeTypes,
      MergeType defaultMergeType,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      MergeResult<CommitLogEntry> adapterMergeResult =
          databaseAdapter.transplant(
              TransplantParams.builder()
                  .toBranch(targetBranch)
                  .expectedHead(referenceHash)
                  .sequenceToTransplant(sequenceToTransplant)
                  .updateCommitMetadata(updateCommitMetadataFunction(updateCommitMetadata))
                  .keepIndividualCommits(keepIndividualCommits)
                  .mergeTypes(mergeTypes)
                  .defaultMergeType(defaultMergeType)
                  .isDryRun(dryRun)
                  .build());
      return storeMergeResult(adapterMergeResult, fetchAdditionalInfo);
    } catch (MergeConflictException mergeConflict) {
      @SuppressWarnings("unchecked")
      MergeResult<CommitLogEntry> adapterMergeResult =
          (MergeResult<CommitLogEntry>) mergeConflict.getMergeResult();
      throw new MergeConflictException(
          mergeConflict.getMessage(), storeMergeResult(adapterMergeResult, fetchAdditionalInfo));
    }
  }

  @Override
  public MergeResult<Commit<METADATA, CONTENT>> merge(
      Hash fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      MetadataRewriter<METADATA> updateCommitMetadata,
      boolean keepIndividualCommits,
      Map<Key, MergeType> mergeTypes,
      MergeType defaultMergeType,
      boolean dryRun,
      boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      MergeResult<CommitLogEntry> adapterMergeResult =
          databaseAdapter.merge(
              MergeParams.builder()
                  .toBranch(toBranch)
                  .expectedHead(expectedHash)
                  .mergeFromHash(fromHash)
                  .updateCommitMetadata(updateCommitMetadataFunction(updateCommitMetadata))
                  .keepIndividualCommits(keepIndividualCommits)
                  .mergeTypes(mergeTypes)
                  .defaultMergeType(defaultMergeType)
                  .isDryRun(dryRun)
                  .build());
      return storeMergeResult(adapterMergeResult, fetchAdditionalInfo);
    } catch (MergeConflictException mergeConflict) {
      @SuppressWarnings("unchecked")
      MergeResult<CommitLogEntry> adapterMergeResult =
          (MergeResult<CommitLogEntry>) mergeConflict.getMergeResult();
      throw new MergeConflictException(
          mergeConflict.getMessage(), storeMergeResult(adapterMergeResult, fetchAdditionalInfo));
    }
  }

  private MergeResult<Commit<METADATA, CONTENT>> storeMergeResult(
      MergeResult<CommitLogEntry> adapterMergeResult, boolean fetchAdditionalInfo) {
    ImmutableMergeResult.Builder<Commit<METADATA, CONTENT>> storeResult =
        ImmutableMergeResult.<Commit<METADATA, CONTENT>>builder()
            .targetBranch(adapterMergeResult.getTargetBranch())
            .effectiveTargetHash(adapterMergeResult.getEffectiveTargetHash())
            .commonAncestor(adapterMergeResult.getCommonAncestor())
            .resultantTargetHash(adapterMergeResult.getResultantTargetHash())
            .expectedHash(adapterMergeResult.getExpectedHash())
            .wasApplied(adapterMergeResult.wasApplied())
            .wasSuccessful(adapterMergeResult.wasSuccessful())
            .details(adapterMergeResult.getDetails());

    BiConsumer<ImmutableCommit.Builder<METADATA, CONTENT>, CommitLogEntry> enhancer =
        enhancerForCommitLog(fetchAdditionalInfo);

    Function<CommitLogEntry, Commit<METADATA, CONTENT>> mapper =
        logEntry -> {
          ImmutableCommit.Builder<METADATA, CONTENT> commit = Commit.builder();
          commit.hash(logEntry.getHash()).commitMeta(deserializeMetadata(logEntry.getMetadata()));
          enhancer.accept(commit, logEntry);
          return commit.build();
        };

    if (adapterMergeResult.getSourceCommits() != null) {
      adapterMergeResult.getSourceCommits().stream()
          .map(mapper)
          .forEach(storeResult::addSourceCommits);
    }
    if (adapterMergeResult.getTargetCommits() != null) {
      adapterMergeResult.getTargetCommits().stream()
          .map(mapper)
          .forEach(storeResult::addTargetCommits);
    }

    return storeResult.build();
  }

  private MetadataRewriter<ByteString> updateCommitMetadataFunction(
      MetadataRewriter<METADATA> updateCommitMetadata) {
    return new MetadataRewriter<ByteString>() {
      @Override
      public ByteString rewriteSingle(ByteString metadata) {
        return serializeMetadata(updateCommitMetadata.rewriteSingle(deserializeMetadata(metadata)));
      }

      @Override
      public ByteString squash(List<ByteString> metadata) {
        return serializeMetadata(
            updateCommitMetadata.squash(
                metadata.stream()
                    .map(PersistVersionStore.this::deserializeMetadata)
                    .collect(Collectors.toList())));
      }
    };
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.assign(ref, expectedHash, targetHash);
  }

  @Override
  public Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return databaseAdapter.create(ref, targetHash.orElseGet(databaseAdapter::noAncestorHash));
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    databaseAdapter.delete(ref, hash);
  }

  @Nonnull
  @Override
  public ReferenceInfo<METADATA> getNamedRef(@Nonnull String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    ReferenceInfo<ByteString> namedRef = databaseAdapter.namedRef(ref, params);
    return namedRef.withUpdatedCommitMeta(deserializeMetadata(namedRef.getHeadCommitMeta()));
  }

  @Override
  public Stream<ReferenceInfo<METADATA>> getNamedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException {
    return databaseAdapter
        .namedRefs(params)
        .map(
            namedRef ->
                namedRef.withUpdatedCommitMeta(deserializeMetadata(namedRef.getHeadCommitMeta())));
  }

  private ByteString serializeMetadata(METADATA metadata) {
    return metadata != null ? storeWorker.getMetadataSerializer().toBytes(metadata) : null;
  }

  private METADATA deserializeMetadata(ByteString commitMeta) {
    return commitMeta != null ? storeWorker.getMetadataSerializer().fromBytes(commitMeta) : null;
  }

  @Override
  public Stream<Commit<METADATA, CONTENT>> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException {
    Hash hash = refToHash(ref);
    Stream<CommitLogEntry> stream = databaseAdapter.commitLog(hash);

    BiConsumer<ImmutableCommit.Builder<METADATA, CONTENT>, CommitLogEntry> enhancer =
        enhancerForCommitLog(fetchAdditionalInfo);

    return stream.map(
        e -> {
          ImmutableCommit.Builder<METADATA, CONTENT> commit =
              Commit.<METADATA, CONTENT>builder()
                  .hash(e.getHash())
                  .commitMeta(deserializeMetadata(e.getMetadata()));
          enhancer.accept(commit, e);
          return commit.build();
        });
  }

  /**
   * Utility function for {@link #getCommits(Ref, boolean)} to optionally enhance the returned
   * {@link Commit} instances with the parent hash and operations per commit.
   */
  private BiConsumer<ImmutableCommit.Builder<METADATA, CONTENT>, CommitLogEntry>
      enhancerForCommitLog(boolean fetchAdditionalInfo) {
    if (!fetchAdditionalInfo) {
      return (commitBuilder, logEntry) -> {
        if (!logEntry.getParents().isEmpty()) {
          commitBuilder.parentHash(logEntry.getParents().get(0));
        }
        logEntry.getAdditionalParents().forEach(commitBuilder::addAdditionalParents);
      };
    }

    // Memoize already retrieved global-content
    Map<ContentId, ByteString> globalContents = new HashMap<>();
    Function<KeyWithBytes, ByteString> getGlobalContents =
        (put) ->
            globalContents.computeIfAbsent(
                put.getContentId(),
                cid ->
                    databaseAdapter
                        .globalContent(put.getContentId())
                        .map(ContentIdAndBytes::getValue)
                        .orElse(null));

    return (commitBuilder, logEntry) -> {
      if (!logEntry.getParents().isEmpty()) {
        commitBuilder.parentHash(logEntry.getParents().get(0));
      }
      logEntry.getDeletes().forEach(delete -> commitBuilder.addOperations(Delete.of(delete)));
      logEntry
          .getPuts()
          .forEach(
              put ->
                  commitBuilder.addOperations(
                      Put.of(
                          put.getKey(),
                          storeWorker.valueFromStore(
                              put.getValue(),
                              () -> getGlobalContents.apply(put),
                              databaseAdapter::mapToAttachment))));
    };
  }

  @Override
  public Stream<KeyEntry<CONTENT_TYPE>> getKeys(Ref ref) throws ReferenceNotFoundException {
    Hash hash = refToHash(ref);
    return databaseAdapter
        .keys(hash, KeyFilterPredicate.ALLOW_ALL)
        .map(
            entry ->
                KeyEntry.of(
                    storeWorker.getType(entry.getType()),
                    entry.getKey(),
                    entry.getContentId().getId()));
  }

  @Override
  public CONTENT getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key)).get(key);
  }

  @Override
  public Map<Key, CONTENT> getValues(Ref ref, Collection<Key> keys)
      throws ReferenceNotFoundException {
    Hash hash = refToHash(ref);
    return databaseAdapter.values(hash, keys, KeyFilterPredicate.ALLOW_ALL).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> mapContentAndState(e.getValue())));
  }

  private CONTENT mapContentAndState(ContentAndState<ByteString> cs) {
    return storeWorker.valueFromStore(
        cs.getRefState(), cs::getGlobalState, databaseAdapter::mapToAttachment);
  }

  @Override
  public Stream<Diff<CONTENT>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    Hash fromHash = refToHash(from);
    Hash toHash = refToHash(to);
    return databaseAdapter
        .diff(fromHash, toHash, KeyFilterPredicate.ALLOW_ALL)
        .map(
            d ->
                Diff.of(
                    d.getKey(),
                    d.getFromValue()
                        .map(
                            v ->
                                storeWorker.valueFromStore(
                                    v,
                                    () -> d.getGlobal().orElse(null),
                                    databaseAdapter::mapToAttachment)),
                    d.getToValue()
                        .map(
                            v ->
                                storeWorker.valueFromStore(
                                    v,
                                    () -> d.getGlobal().orElse(null),
                                    databaseAdapter::mapToAttachment))));
  }

  private Hash refToHash(Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof NamedRef) {
      return hashOnReference((NamedRef) ref, Optional.empty());
    }
    if (ref instanceof Hash) {
      return (Hash) ref;
    }
    throw new IllegalArgumentException(String.format("Unsupported reference '%s'", ref));
  }

  @Override
  public Stream<RefLogDetails> getRefLog(Hash refLogId) throws RefLogNotFoundException {
    Stream<RefLog> refLogStream = databaseAdapter.refLog(refLogId);
    return refLogStream.map(
        e ->
            ImmutableRefLogDetails.builder()
                .refLogId(e.getRefLogId())
                .refName(e.getRefName())
                .refType(e.getRefType())
                .commitHash(e.getCommitHash())
                .parentRefLogId(e.getParents().get(0))
                .operationTime(e.getOperationTime())
                .operation(e.getOperation())
                .sourceHashes(e.getSourceHashes())
                .build());
  }
}
