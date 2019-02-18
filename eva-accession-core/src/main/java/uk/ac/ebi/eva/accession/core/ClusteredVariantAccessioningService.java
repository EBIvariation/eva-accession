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
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

/**
 * Service entry point for accessioning and querying clustered variants.
 *
 * TODO Support EVA clustered variants, see {@link SubmittedVariantAccessioningService} for reference
 */
public class ClusteredVariantAccessioningService extends BasicAccessioningService<IClusteredVariant, String, Long> {

    public ClusteredVariantAccessioningService(DbsnpMonotonicAccessionGenerator<IClusteredVariant> generator,
                                               DatabaseService<IClusteredVariant, String, Long> dbServiceDbsnp) {
        super(generator, dbServiceDbsnp, new ClusteredVariantSummaryFunction(), new SHA1HashingFunction());
    }

}