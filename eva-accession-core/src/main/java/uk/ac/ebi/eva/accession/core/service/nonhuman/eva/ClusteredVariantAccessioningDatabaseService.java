/*
 *
 * Copyright 2020 EMBL - European Bioinformatics Institute
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

import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;

import java.util.List;
import java.util.stream.Collectors;

public class ClusteredVariantAccessioningDatabaseService extends
        BasicSpringDataRepositoryMonotonicDatabaseService<IClusteredVariant, ClusteredVariantEntity> {

    private final ClusteredVariantAccessioningRepository repository;

    private ClusteredVariantInactiveService inactiveService;

    public ClusteredVariantAccessioningDatabaseService(ClusteredVariantAccessioningRepository repository,
                                                       ClusteredVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new ClusteredVariantEntity(accessionWrapper.getAccession(),
                                                             accessionWrapper.getHash(),
                                                             accessionWrapper.getData(),
                                                             accessionWrapper.getVersion()),
              inactiveService);
        this.repository = repository;
        this.inactiveService = inactiveService;
    }


    /**
     * Note that at the end of this method we delete and insert instead of just updating the rs accession.
     *
     * This is because there is no "update" method in the CrudRepository; only "save", which will call
     * ClusteredVariantEntity#isNew() to decide whether to insert a new document or update an existing one. With the
     * current code, a repositoyr.save() would fail with an _id collision because isNew() is hardcoded to true. Trying
     * to make that work involves non-trivial code, like created another class that inherits and overrides
     * {@link ClusteredVariantEntity#isNew()}.
     *
     * @see SimpleMongoRepository#save(java.lang.Object)
     *
     * Other alternatives which (at the moment of writing) seem worse than the current implementation:
     *
     * We could bypass this repository limitation by using a mongoTemplate and using the "update" method (which is
     * what we need) but that defeats the purpose of the accession-commons class design (AccessioningService,
     * DatabaseService, etc.). We only have repositories here, which could be configured in different ways that we don't
     * know here.
     *
     * Another option would be to extract the "mergeKeepingEntries" out of the DatabaseService. After all, we need
     * to provide this merging mechanism across dbsnp and EVA.
     */
    public void mergeKeepingEntries(Long accessionOrigin, Long mergeInto, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<ClusteredVariantEntity> toMerge = this.getAllByAccession(accessionOrigin);
        this.getAllByAccession(mergeInto); // trigger checks for inactive object
        inactiveService.merge(accessionOrigin, mergeInto, toMerge, reason);

        List<ClusteredVariantEntity> updated = toMerge.stream()
                                                      .map(e -> new ClusteredVariantEntity(
                                                              mergeInto,
                                                              e.getHashedMessage(),
                                                              e.getModel()))
                                                      .collect(Collectors.toList());
        repository.deleteAll(toMerge);
        repository.saveAll(updated);
    }

    public List<ClusteredVariantEntity> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<ClusteredVariantEntity> entities = this.repository.findByAccession(accession);
        this.checkAccessionIsActive(entities, accession);
        return entities;
    }

    private void checkAccessionIsActive(List<ClusteredVariantEntity> entities, Long accession)
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
                                               this.inactiveService.getLastEvent(accession).getMergedInto().toString());
        case DEPRECATED:
            throw new AccessionDeprecatedException(accession.toString());
        default:
        }
    }


}
