/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.ai.temporal.integration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.geaflow.ai.temporal.model.Evidence;
import org.apache.geaflow.ai.temporal.model.FactValue;
import org.apache.geaflow.ai.temporal.model.MemoryEventOperation;
import org.apache.geaflow.ai.temporal.model.MemoryFact;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersion;
import org.apache.geaflow.ai.temporal.model.MemoryFactVersionStatus;
import org.apache.geaflow.ai.temporal.model.TimeInterval;
import org.apache.geaflow.ai.temporal.model.VersionRelation;
import org.apache.geaflow.ai.temporal.model.VersionRelationType;
import org.apache.geaflow.ai.temporal.semantics.NormalizedMemoryEvent;
import org.apache.geaflow.ai.temporal.semantics.TemporalErrorCode;
import org.apache.geaflow.ai.temporal.semantics.TemporalValidationException;
import org.apache.geaflow.ai.temporal.semantics.VersionIdentity;

/** Resolves normalized same-key events with deterministic temporal rules. */
public final class ConflictResolver {

    private static final Comparator<MemoryFactVersion> VALID_TIME_ORDER =
        Comparator.comparing((MemoryFactVersion version) -> version.getValidTime().getStart())
            .thenComparing(MemoryFactVersion::getId);

    private static final Comparator<Evidence> EVIDENCE_ORDER =
        Comparator.comparing(Evidence::getId)
            .thenComparing(evidence -> evidence.getSource().getId())
            .thenComparing(evidence -> evidence.getSource().getName())
            .thenComparing(Evidence::getContent);

    /** The input must contain the complete state partition for the event's fact key. */
    public ResolutionDecision resolve(NormalizedMemoryEvent event,
                                      List<MemoryFactVersion> sameKeyVersions) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(sameKeyVersions, "sameKeyVersions");
        if (event.getOperation() == MemoryEventOperation.ADD) {
            return resolveAdd(event, sameKeyVersions);
        }
        if (event.getOperation() == MemoryEventOperation.CORRECT
            || event.getOperation() == MemoryEventOperation.RETRACT) {
            return resolveChange(event, sameKeyVersions);
        }
        throw new UnsupportedOperationException(
            "Unsupported memory event operation: " + event.getOperation());
    }

    private static ResolutionDecision resolveAdd(NormalizedMemoryEvent event,
                                                 List<MemoryFactVersion> versions) {
        final List<MemoryFactVersion> duplicates = new ArrayList<>();
        TimeInterval validTime = event.getValidTime();
        boolean expanded;
        do {
            expanded = false;
            for (MemoryFactVersion version : versions) {
                if (isCurrentActive(version)
                    && !duplicates.contains(version)
                    && version.getValidTime().overlaps(validTime)
                    && hasSameValue(event, version)) {
                    duplicates.add(version);
                    validTime = span(validTime, version.getValidTime());
                    expanded = true;
                }
            }
        } while (expanded);
        Collections.sort(duplicates, VALID_TIME_ORDER);
        final List<MemoryFactVersion> mandatory = activeOverlaps(versions, validTime);
        final List<MemoryFactVersion> conflicts = new ArrayList<>(mandatory);
        conflicts.removeAll(duplicates);
        final List<String> rules = new ArrayList<>();
        rules.add(duplicates.isEmpty() ? "ASSERT_NEW" : "MERGE_EQUAL_VALUE");
        if (!conflicts.isEmpty()) {
            rules.add("COEXIST_CONFLICT");
        }
        return new ResolutionDecision(validTime, mandatory, mandatory, rules,
            duplicates, conflicts, null);
    }

    private static ResolutionDecision resolveChange(NormalizedMemoryEvent event,
                                                    List<MemoryFactVersion> versions) {
        final String targetId = event.getTargetVersionId().orElseThrow(() ->
            new TemporalValidationException(TemporalErrorCode.REJECT_TARGET_REQUIRED,
                "CORRECT/RETRACT requires a target version"));
        MemoryFactVersion target = null;
        for (MemoryFactVersion candidate : versions) {
            if (candidate.getId().equals(targetId)) {
                target = candidate;
                break;
            }
        }
        if (target == null) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_TARGET_NOT_FOUND,
                "Target is not in this fact key: " + targetId);
        }
        if (!isCurrentActive(target)) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_TARGET_INACTIVE,
                "Target is not current and active: " + targetId);
        }
        if (!target.getValidTime().overlaps(event.getValidTime())) {
            throw new TemporalValidationException(TemporalErrorCode.REJECT_TARGET_NO_OVERLAP,
                "Target and requested valid intervals do not overlap");
        }
        TimeInterval intersection = target.getValidTime()
            .intersection(event.getValidTime()).get();
        return new ResolutionDecision(intersection, activeOverlaps(versions, intersection),
            Collections.singletonList(target), Collections.singletonList(
                event.getOperation() == MemoryEventOperation.CORRECT
                    ? "CORRECT_TARGET" : "RETRACT_TARGET"),
            Collections.emptyList(), Collections.emptyList(), target);
    }

    private static List<MemoryFactVersion> activeOverlaps(List<MemoryFactVersion> versions,
                                                         TimeInterval interval) {
        final List<MemoryFactVersion> result = new ArrayList<>();
        for (MemoryFactVersion version : versions) {
            if (isCurrentActive(version) && version.getValidTime().overlaps(interval)) {
                result.add(version);
            }
        }
        Collections.sort(result, VALID_TIME_ORDER);
        return result;
    }

    public void apply(
        NormalizedMemoryEvent event,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        final ResolutionDecision decision = resolve(event, versions);
        final MemoryEventOperation operation = event.getOperation();
        if (operation == MemoryEventOperation.ADD) {
            applyNormalizedAdd(event, decision, versions, relations, generatingEventIds);
        } else if (operation == MemoryEventOperation.CORRECT
            || operation == MemoryEventOperation.RETRACT) {
            applyNormalizedChange(event, decision, versions, relations, generatingEventIds);
        } else {
            throw new UnsupportedOperationException(
                "Unsupported memory event operation: "
                    + operation);
        }
    }

    private static void applyNormalizedAdd(
        NormalizedMemoryEvent event,
        ResolutionDecision decision,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        final List<MemoryFactVersion> duplicates = decision.getDuplicateVersions();
        TimeInterval validTime = decision.getEffectiveValidTime();

        final List<Evidence> evidence = new ArrayList<>();
        mergeEvidence(evidence, event.getEvidence());
        final Map<String, Boolean> materializedDuplicates = new HashMap<>();
        for (MemoryFactVersion duplicate : duplicates) {
            versions.remove(duplicate);
            boolean materialized = materializeClosedVersion(
                duplicate,
                event.getRecordedAt(),
                versions,
                relations,
                generatingEventIds);
            materializedDuplicates.put(duplicate.getId(), materialized);
            mergeEvidence(evidence, duplicate.getEvidence());
        }

        MemoryFactVersion added = new MemoryFactVersion(
            VersionIdentity.create(event, event.getEvent().getFact().get(),
                MemoryFactVersionStatus.ACTIVE, validTime, null),
            event.getEvent().getFact().get(),
            MemoryFactVersionStatus.ACTIVE,
            validTime,
            TimeInterval.unboundedFrom(event.getRecordedAt()),
            evidence);
        versions.add(added);
        generatingEventIds.put(added.getId(), event.getEventId());

        for (MemoryFactVersion duplicate : duplicates) {
            if (Boolean.TRUE.equals(
                materializedDuplicates.get(duplicate.getId()))) {
                addRelation(
                    relations,
                    new VersionRelation(
                        VersionRelationType.DUPLICATE_OF,
                        added.getId(),
                        duplicate.getId()));
            }
        }

        for (MemoryFactVersion version : decision.getConflictVersions()) {
            addRelation(relations, new VersionRelation(VersionRelationType.CONFLICTS_WITH,
                added.getId(), version.getId()));
        }
    }

    private static void applyNormalizedChange(
        NormalizedMemoryEvent event,
        ResolutionDecision decision,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        final String targetId = decision.getTargetVersionId();
        final MemoryFactVersion target = decision.getTargetVersion();
        final TimeInterval intersection = decision.getEffectiveValidTime();
        versions.remove(target);
        final Map<String, Boolean> materialized = new HashMap<>();
        materialized.put(targetId, materializeClosedVersion(target,
            event.getRecordedAt(), versions, relations, generatingEventIds));
        boolean correcting = event.getOperation() == MemoryEventOperation.CORRECT;
        final MemoryFact replacementFact = correcting
            ? event.getEvent().getFact().get() : target.getFact();
        final MemoryFactVersionStatus replacementStatus = correcting
            ? MemoryFactVersionStatus.ACTIVE : MemoryFactVersionStatus.RETRACTED;
        MemoryFactVersion replacement = new MemoryFactVersion(
            VersionIdentity.create(event, replacementFact, replacementStatus,
                intersection, targetId),
            replacementFact, replacementStatus, intersection,
            TimeInterval.unboundedFrom(event.getRecordedAt()), event.getEvidence());
        versions.add(replacement);
        generatingEventIds.put(replacement.getId(), event.getEventId());
        addSupersedesIfMaterialized(replacement, target, materialized, relations);
        for (TimeInterval remainder : target.getValidTime().subtract(intersection)) {
            MemoryFactVersion residual = new MemoryFactVersion(
                VersionIdentity.create(event, target.getFact(),
                    MemoryFactVersionStatus.ACTIVE, remainder, targetId),
                target.getFact(), MemoryFactVersionStatus.ACTIVE, remainder,
                TimeInterval.unboundedFrom(event.getRecordedAt()), target.getEvidence());
            versions.add(residual);
            generatingEventIds.put(residual.getId(), event.getEventId());
            addSupersedesIfMaterialized(residual, target, materialized, relations);
        }
        addCurrentConflicts(versions, relations);
    }

    private static void addCurrentConflicts(
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations) {
        for (int leftIndex = 0;
            leftIndex < versions.size(); leftIndex++) {
            MemoryFactVersion left = versions.get(leftIndex);
            if (!isCurrentActive(left)) {
                continue;
            }
            for (int rightIndex = leftIndex + 1;
                rightIndex < versions.size(); rightIndex++) {
                MemoryFactVersion right = versions.get(rightIndex);
                if (isCurrentActive(right)
                    && left.getValidTime().overlaps(
                        right.getValidTime())
                    && !factValue(left.getFact()).equals(
                        factValue(right.getFact()))) {
                    addRelation(
                        relations,
                        new VersionRelation(
                            VersionRelationType.CONFLICTS_WITH,
                            left.getId(),
                            right.getId()));
                }
            }
        }
    }

    private static boolean materializeClosedVersion(
        MemoryFactVersion version,
        Instant recordedAt,
        List<MemoryFactVersion> versions,
        List<VersionRelation> relations,
        Map<String, String> generatingEventIds) {
        if (!version.getTransactionTime().getStart()
            .isBefore(recordedAt)) {
            removeRelationsFor(version.getId(), relations);
            generatingEventIds.remove(version.getId());
            return false;
        }

        versions.add(new MemoryFactVersion(
            version.getId(),
            version.getFact(),
            version.getStatus(),
            version.getValidTime(),
            new TimeInterval(
                version.getTransactionTime().getStart(),
                recordedAt),
            version.getEvidence()));
        return true;
    }

    private static void addSupersedesIfMaterialized(
        MemoryFactVersion replacement,
        MemoryFactVersion source,
        Map<String, Boolean> materializedSources,
        List<VersionRelation> relations) {
        if (Boolean.TRUE.equals(
            materializedSources.get(source.getId()))) {
            addRelation(
                relations,
                new VersionRelation(
                    VersionRelationType.SUPERSEDES,
                    replacement.getId(),
                    source.getId()));
        }
    }

    private static void addRelation(
        List<VersionRelation> relations,
        VersionRelation relation) {
        if (!relations.contains(relation)) {
            relations.add(relation);
        }
    }

    private static void removeRelationsFor(
        String versionId,
        List<VersionRelation> relations) {
        for (int index = relations.size() - 1; index >= 0; index--) {
            VersionRelation relation = relations.get(index);
            if (relation.getFromVersionId().equals(versionId)
                || relation.getToVersionId().equals(versionId)) {
                relations.remove(index);
            }
        }
    }

    private static boolean hasSameValue(
        NormalizedMemoryEvent event,
        MemoryFactVersion version) {
        final Optional<FactValue> eventValue = event.getFactValue();
        return eventValue.isPresent()
            && eventValue.get().equals(factValue(version.getFact()));
    }

    private static FactValue factValue(MemoryFact fact) {
        if (fact.isRelationship()) {
            return FactValue.entityReference(
                fact.getTarget().get().getId());
        }
        return FactValue.literal(fact.getLiteralValue().get());
    }

    private static TimeInterval span(
        TimeInterval left,
        TimeInterval right) {
        Instant start = left.getStart().isBefore(right.getStart())
            ? left.getStart() : right.getStart();
        Instant end;
        if (!left.getEnd().isPresent()
            || !right.getEnd().isPresent()) {
            end = null;
        } else {
            Instant leftEnd = left.getEnd().get();
            Instant rightEnd = right.getEnd().get();
            end = leftEnd.isAfter(rightEnd) ? leftEnd : rightEnd;
        }
        return new TimeInterval(start, end);
    }

    private static void mergeEvidence(
        List<Evidence> target,
        List<Evidence> additions) {
        for (Evidence evidence : additions) {
            if (!target.contains(evidence)) {
                target.add(evidence);
            }
        }
        Collections.sort(target, EVIDENCE_ORDER);
    }

    private static boolean isCurrentActive(
        MemoryFactVersion version) {
        return version.getStatus() == MemoryFactVersionStatus.ACTIVE
            && isCurrent(version);
    }

    private static boolean isCurrent(MemoryFactVersion version) {
        return !version.getTransactionTime().getEnd().isPresent();
    }
}
