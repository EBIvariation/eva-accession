/*
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
 */
package uk.ac.ebi.eva.accession.core.service;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningDatabaseService;

import java.util.List;
import java.util.function.Function;

public class SubmittedVariantMonotonicAccessioningService
        extends BasicAccessioningService<ISubmittedVariant, String, Long> {

    private final SubmittedVariantAccessioningDatabaseService dbService;

    public SubmittedVariantMonotonicAccessioningService(
            MonotonicAccessionGenerator<ISubmittedVariant> accessionGenerator,
            SubmittedVariantAccessioningDatabaseService dbService,
            Function<ISubmittedVariant, String> summaryFunction,
            Function<String, String> hashingFunction) {
        super(accessionGenerator, dbService, summaryFunction, hashingFunction);
        this.dbService = dbService;
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getByClusteredVariantAccessionIn(
            List<Long> clusteredVariantAccessions) {
        return dbService.findByClusteredVariantAccessionIn(clusteredVariantAccessions);
    }


    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        return dbService.getLastInactive(accession);
    }
}
