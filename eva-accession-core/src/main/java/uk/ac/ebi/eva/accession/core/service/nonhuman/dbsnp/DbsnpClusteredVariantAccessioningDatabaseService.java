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
package uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.IAccessionedObject;
import uk.ac.ebi.ampt2d.commons.accession.persistence.services.InactiveAccessionService;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class DbsnpClusteredVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryMonotonicDatabaseService<IClusteredVariant, DbsnpClusteredVariantEntity> {

    private final DbsnpClusteredVariantAccessioningRepository repository;

    private final DbsnpClusteredVariantInactiveService inactiveService;

    public DbsnpClusteredVariantAccessioningDatabaseService(DbsnpClusteredVariantAccessioningRepository repository,
                                                            DbsnpClusteredVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new DbsnpClusteredVariantEntity(accessionWrapper.getAccession(),
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

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<DbsnpClusteredVariantEntity> entities = this.repository.findByAccession(accession);
        this.checkAccessionIsActive(entities, accession);
        return entities.stream().map(this::toModelWrapper).collect(Collectors.toList());
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllActiveByAssemblyAndAccessionIn
            (String assembly, List<Long> accessionList) {
        List<DbsnpClusteredVariantEntity> entities = this.repository
                .findByAssemblyAccessionAndAccessionIn(assembly, accessionList);
        return entities.stream().map(this::toModelWrapper).collect(Collectors.toList());
    }

    private AccessionWrapper<IClusteredVariant, String, Long> toModelWrapper(DbsnpClusteredVariantEntity entity) {
        return new AccessionWrapper(entity.getAccession(), entity.getHashedMessage(), entity.getModel(), entity.getVersion());
    }

    private void checkAccessionIsActive(List<DbsnpClusteredVariantEntity> entities, Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        if (entities == null || entities.isEmpty()) {
            this.checkAccessionNotMergedOrDeprecated(accession);
        }
    }

    private void checkAccessionNotMergedOrDeprecated(Long accession)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        EventType eventType = this.inactiveService.getLastEventType(accession).orElseThrow(() -> {
            return new AccessionDoesNotExistException(accession.toString());
        });
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

    public AccessionWrapper<IClusteredVariant, String, Long> getLastInactive(Long accession) {
        IEvent<IClusteredVariant, Long> lastEvent = ((InactiveAccessionService<IClusteredVariant, Long, ?>) inactiveService)
                .getLastEvent(accession);
        List<? extends IAccessionedObject<IClusteredVariant, ?, Long>> inactiveObjects = lastEvent.getInactiveObjects();
        if (inactiveObjects.isEmpty()) {
            throw new IllegalArgumentException(
                    "Accession " + accession + " is not inactive (not present in the operations collection");
        }
        IAccessionedObject<IClusteredVariant, ?, Long> inactiveObject = inactiveObjects.get(inactiveObjects.size() - 1);
        return new AccessionWrapper<>(accession, (String) inactiveObject.getHashedMessage(), inactiveObject.getModel(),
                inactiveObject.getVersion());
    }

}
