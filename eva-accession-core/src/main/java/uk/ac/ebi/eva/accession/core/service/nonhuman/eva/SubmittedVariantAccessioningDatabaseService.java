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
package uk.ac.ebi.eva.accession.core.service.nonhuman.eva;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.IAccessionedObject;
import uk.ac.ebi.ampt2d.commons.accession.persistence.services.InactiveAccessionService;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SubmittedVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryMonotonicDatabaseService<ISubmittedVariant, SubmittedVariantEntity> {

    private final SubmittedVariantAccessioningRepository repository;

    private SubmittedVariantInactiveService inactiveService;

    public SubmittedVariantAccessioningDatabaseService(SubmittedVariantAccessioningRepository repository,
                                                       SubmittedVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new SubmittedVariantEntity(accessionWrapper.getAccession(),
                                                             accessionWrapper.getHash(),
                                                             accessionWrapper.getData(),
                                                             accessionWrapper.getVersion()),
              inactiveService);
        this.repository = repository;
        this.inactiveService = inactiveService;
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> findByClusteredVariantAccessionIn(
            List<Long> clusteredVariantIds) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> wrappedAccessions = new ArrayList<>();
        repository.findByClusteredVariantAccessionIn(clusteredVariantIds).iterator().forEachRemaining(
                entity -> wrappedAccessions.add(toModelWrapper(entity)));
        return wrappedAccessions;
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> findByHashedMessageIn(
            List<String> hashes) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> wrappedAccessions = new ArrayList<>();
        repository.findByHashedMessageIn(hashes).iterator().forEachRemaining(
                entity -> wrappedAccessions.add(toModelWrapper(entity)));
        return wrappedAccessions;
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> toModelWrapper(SubmittedVariantEntity entity) {
        return new AccessionWrapper<>(entity.getAccession(), entity.getHashedMessage(), entity.getModel(),
                                      entity.getVersion());
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        IEvent<ISubmittedVariant, Long> lastEvent = ((InactiveAccessionService<ISubmittedVariant, Long, ?>) inactiveService)
                .getLastEvent(accession);
        List<? extends IAccessionedObject<ISubmittedVariant, ?, Long>> inactiveObjects = lastEvent.getInactiveObjects();
        if (inactiveObjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "Accession " + accession + " is not inactive (not present in the operations collection");
        }
        IAccessionedObject<ISubmittedVariant, ?, Long> inactiveObject = inactiveObjects.get(inactiveObjects.size() - 1);
        return new AccessionWrapper<>(accession, (String) inactiveObject.getHashedMessage(), inactiveObject.getModel(),
                                      inactiveObject.getVersion());
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<SubmittedVariantEntity> entities = this.repository.findByAccession(accession);
        this.checkAccessionIsActive(entities, accession);
        return entities.stream().map(this::toModelWrapper).collect(Collectors.toList());
    }

    private void checkAccessionIsActive(List<SubmittedVariantEntity> entities, Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        if (entities == null || entities.isEmpty()) {
            this.checkAccessionNotMergedOrDeprecated(accession);
        }
    }

    private void checkAccessionNotMergedOrDeprecated(Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        EventType eventType = this.inactiveService.getLastEventType(accession).orElseThrow(() ->
                new AccessionDoesNotExistException(accession.toString())
        );
        switch(eventType) {
            case MERGED:
                throw new AccessionMergedException(accession.toString(),
                        this.inactiveService.getLastEvent(accession)
                                .getMergedInto().toString());
            case DEPRECATED:
                throw new AccessionDeprecatedException(accession.toString());
            default:
        }
    }
}
