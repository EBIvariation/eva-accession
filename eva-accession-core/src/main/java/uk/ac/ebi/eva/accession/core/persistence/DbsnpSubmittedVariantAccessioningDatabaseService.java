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

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.MonotonicDatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.persistence.services.BasicSpringDataRepositoryDatabaseService;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantInactiveService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DbsnpSubmittedVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryDatabaseService<ISubmittedVariant, Long, DbsnpSubmittedVariantEntity>
        implements MonotonicDatabaseService<ISubmittedVariant, String> {


    private final DbsnpSubmittedVariantAccessioningRepository repository;

    public DbsnpSubmittedVariantAccessioningDatabaseService(DbsnpSubmittedVariantAccessioningRepository repository,
                                                            DbsnpSubmittedVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new DbsnpSubmittedVariantEntity(accessionWrapper.getAccession(),
                                                                  accessionWrapper.getHash(),
                                                                  accessionWrapper.getData(),
                                                                  accessionWrapper.getVersion()),
              inactiveService);
        this.repository = repository;
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

}
