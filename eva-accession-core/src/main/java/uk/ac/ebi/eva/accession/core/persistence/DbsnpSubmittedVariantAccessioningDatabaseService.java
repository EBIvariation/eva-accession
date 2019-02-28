/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package uk.ac.ebi.eva.accession.core.persistence;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionVersionsWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantInactiveService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DbsnpSubmittedVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryMonotonicDatabaseService<ISubmittedVariant, DbsnpSubmittedVariantEntity> {

    private final DbsnpSubmittedVariantAccessioningRepository repository;

    private final DbsnpSubmittedVariantInactiveService inactiveService;

    public DbsnpSubmittedVariantAccessioningDatabaseService(DbsnpSubmittedVariantAccessioningRepository repository,
                                                            DbsnpSubmittedVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new DbsnpSubmittedVariantEntity(accessionWrapper.getAccession(),
                                                                  accessionWrapper.getHash(),
                                                                  accessionWrapper.getData(),
                                                                  accessionWrapper.getVersion()),
              inactiveService);
        this.repository = repository;
        this.inactiveService = inactiveService;
    }

    @Override
    public long[] getAccessionsInRanges(Collection<MonotonicRange> ranges) {
        throw new UnsupportedOperationException("New accessions cannot be issued for dbSNP variants");
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> findByClusteredVariantAccessionIn(
            List<Long> clusteredVariantIds) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> wrappedAccessions = new ArrayList<>();
        repository.findByClusteredVariantAccessionIn(clusteredVariantIds).iterator().forEachRemaining(
                entity -> wrappedAccessions.add(toModelWrapper(entity)));
        return wrappedAccessions;
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> toModelWrapper(DbsnpSubmittedVariantEntity entity) {
        return new AccessionWrapper<>(entity.getAccession(), entity.getHashedMessage(), entity.getModel(),
                                      entity.getVersion());
    }

    @Override
    public AccessionVersionsWrapper<ISubmittedVariant, String, Long> findByAccession(
            Long accession) throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        List<DbsnpSubmittedVariantEntity> entities = this.repository.findByAccession(accession);
        this.checkAccessionIsActive(entities, accession);
        return this.toAccessionWrapper(entities);
    }

    /**
     * if entities is empty it means that the accession is not present in the main collection. Check if it was moved
     * to the history collection after it was merged or deprecated, and if so, throw an exception to flag that it's not
     * an active accession.
     */
    private void checkAccessionIsActive(List<DbsnpSubmittedVariantEntity> entities, Long accession)
            throws AccessionMergedException, AccessionDeprecatedException {
        if (entities == null || entities.isEmpty()) {
            this.checkAccessionIsNotMergedOrDeprecated(accession);
        }
    }

    /**
     * dbSNP submitted variants can be "updated" and "merged" at the same time. Give priority to the "merge" events. No more
     * than 1 "merge" events will be present.
     */
    private void checkAccessionIsNotMergedOrDeprecated(Long accession)
            throws AccessionMergedException, AccessionDeprecatedException {
        List<? extends IEvent<ISubmittedVariant, Long>> events = inactiveService.getEvents(accession);
        Optional<? extends IEvent<ISubmittedVariant, Long>> mergedEvent = events.stream()
                                                                                .filter(e -> EventType.MERGED.equals(
                                                                                        e.getEventType()))
                                                                                .findFirst();
        if (mergedEvent.isPresent()) {
            throw new AccessionMergedException(accession.toString(), mergedEvent.get().getMergedInto().toString());
        } else if (events.stream().map(IEvent::getEventType).anyMatch(EventType.DEPRECATED::equals)) {
            throw new AccessionDeprecatedException(accession.toString());
        }
    }

    private AccessionVersionsWrapper<ISubmittedVariant, String, Long> toAccessionWrapper(List<DbsnpSubmittedVariantEntity> entities) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> models = entities.stream()
                                                                                 .map(this::toModelWrapper)
                                                                                 .collect(Collectors.toList());
        return new AccessionVersionsWrapper<>(models);
    }

    @Override
    public AccessionWrapper<ISubmittedVariant, String, Long> findLastVersionByAccession(Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        List<DbsnpSubmittedVariantEntity> entities = this.repository.findByAccession(accession);
        this.checkAccessionIsActive(entities, accession);
        return this.toModelWrapper(this.getNewestVersion(entities));
    }

    private DbsnpSubmittedVariantEntity getNewestVersion(List<DbsnpSubmittedVariantEntity> accessionedElements) {
        int maxVersion = 1;
        DbsnpSubmittedVariantEntity lastVersionEntity = null;
        for (DbsnpSubmittedVariantEntity element : accessionedElements) {
            if (element.getVersion() >= maxVersion) {
                maxVersion = element.getVersion();
                lastVersionEntity = element;
            }
        }
        return lastVersionEntity;
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        return SubmittedVariantAccessioningDatabaseService.getLastInactive(accession, inactiveService);
    }
}
