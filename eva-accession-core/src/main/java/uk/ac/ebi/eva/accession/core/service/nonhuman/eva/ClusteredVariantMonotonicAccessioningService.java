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

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;

import java.util.List;
import java.util.function.Function;

public class ClusteredVariantMonotonicAccessioningService
        extends BasicAccessioningService<IClusteredVariant, String, Long> {

    private final ClusteredVariantAccessioningDatabaseService dbService;

    private final Function<IClusteredVariant, String> hashingFunction;

    public ClusteredVariantMonotonicAccessioningService(
            MonotonicAccessionGenerator<IClusteredVariant> accessionGenerator,
            ClusteredVariantAccessioningDatabaseService dbService,
            Function<IClusteredVariant, String> summaryFunction,
            Function<String, String> hashingFunction) {
        super(accessionGenerator, dbService, summaryFunction, hashingFunction);
        this.dbService = dbService;
        this.hashingFunction = summaryFunction.andThen(hashingFunction);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByHash(List<String> hashes) {
        return dbService.findAllByHash(hashes);
    }

    public void mergeKeepingEntries(Long accessionOrigin, Long mergeInto, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        dbService.mergeKeepingEntries(accessionOrigin, mergeInto, reason);
    }
}
