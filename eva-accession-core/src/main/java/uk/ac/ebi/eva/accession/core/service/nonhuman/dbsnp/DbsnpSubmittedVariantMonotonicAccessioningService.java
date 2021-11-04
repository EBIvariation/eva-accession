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
package uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;

import java.util.List;
import java.util.function.Function;

public class DbsnpSubmittedVariantMonotonicAccessioningService
        extends BasicAccessioningService<ISubmittedVariant, String, Long> {

    private final DbsnpSubmittedVariantAccessioningDatabaseService dbService;

    private final Function<ISubmittedVariant, String> hashingFunction;

    public DbsnpSubmittedVariantMonotonicAccessioningService(
            MonotonicAccessionGenerator<ISubmittedVariant> accessionGenerator,
            DbsnpSubmittedVariantAccessioningDatabaseService dbService,
            Function<ISubmittedVariant, String> summaryFunction,
            Function<String, String> hashingFunction) {
        super(accessionGenerator, dbService, summaryFunction, hashingFunction);
        this.hashingFunction = summaryFunction.andThen(hashingFunction);
        this.dbService = dbService;
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getByClusteredVariantAccessionIn(
            List<Long> clusteredVariantAccessions) {
        return dbService.findByClusteredVariantAccessionIn(clusteredVariantAccessions);
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        return dbService.getLastInactive(accession);
    }

    public String getHash(ISubmittedVariant message) {
        return this.hashingFunction.apply(message);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return dbService.getAllByAccession(accession);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>>
    getAllActiveByAssemblyAndAccessionIn(String assembly, List<Long> accessionList) {
        return dbService.getAllActiveByAssemblyAndAccessionIn(assembly, accessionList);
    }
}
