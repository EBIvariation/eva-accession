/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
 */
package uk.ac.ebi.eva.accession.core.service.human.dbsnp;

import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantAccessionRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;

import java.util.Collection;

public class HumanDbsnpClusteredVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryMonotonicDatabaseService<IClusteredVariant, DbsnpClusteredVariantEntity> {

    public HumanDbsnpClusteredVariantAccessioningDatabaseService(
            HumanDbsnpClusteredVariantAccessionRepository repository,
            DbsnpClusteredVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new DbsnpClusteredVariantEntity(accessionWrapper.getAccession(),
                                                                  accessionWrapper.getHash(),
                                                                  accessionWrapper.getData(),
                                                                  accessionWrapper.getVersion()),
              inactiveService);
    }

    @Override
    public long[] getAccessionsInRanges(Collection<MonotonicRange> ranges) {
        throw new UnsupportedOperationException("New accessions cannot be issued for dbSNP variants");
    }

    //TODO: findAllByAccession
}
