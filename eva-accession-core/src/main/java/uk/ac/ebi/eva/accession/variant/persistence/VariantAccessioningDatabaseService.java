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
package uk.ac.ebi.eva.accession.variant.persistence;


import uk.ac.ebi.eva.accession.variant.VariantModel;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.persistence.BasicSpringDataRepositoryDatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.persistence.monotonic.service.MonotonicDatabaseService;

import java.util.Collection;

public class VariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryDatabaseService<VariantModel, VariantEntity, String, Long>
        implements MonotonicDatabaseService<VariantModel, String> {

    public VariantAccessioningDatabaseService(VariantAccessioningRepository repository) {
        super(repository,
              variantModelHashAccession -> new VariantEntity(variantModelHashAccession.accession(),
                                                             variantModelHashAccession.hash(),
                                                             variantModelHashAccession.model()),
              variantEntity -> variantEntity.getAccession(),
              variantEntity -> variantEntity.getHashedMessage());
    }

    @Override
    public long[] getAccessionsInRanges(Collection<MonotonicRange> ranges) {
        return new long[0];
    }
}
