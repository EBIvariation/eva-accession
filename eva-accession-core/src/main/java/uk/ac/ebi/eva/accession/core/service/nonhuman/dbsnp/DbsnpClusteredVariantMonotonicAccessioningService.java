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
package uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;

import java.util.List;
import java.util.function.Function;

public class DbsnpClusteredVariantMonotonicAccessioningService
        extends BasicAccessioningService<IClusteredVariant, String, Long> {

    private final DbsnpClusteredVariantAccessioningDatabaseService dbService;

    private final Function<IClusteredVariant, String> hashingFunction;

    public DbsnpClusteredVariantMonotonicAccessioningService(
            MonotonicAccessionGenerator<IClusteredVariant> accessionGenerator,
            DbsnpClusteredVariantAccessioningDatabaseService dbService,
            Function<IClusteredVariant, String> summaryFunction,
            Function<String, String> hashingFunction) {
        super(accessionGenerator, dbService, summaryFunction, hashingFunction);

        this.dbService = dbService;
        this.hashingFunction = summaryFunction.andThen(hashingFunction);
    }

    public String getHash(IClusteredVariant variant) {
        return this.hashingFunction.apply(variant);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByHash(List<String> hashes) {
        return dbService.findAllByHash(hashes);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return dbService.getAllByAccession(accession);
    }
}
