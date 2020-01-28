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
package uk.ac.ebi.eva.accession.core.service.nonhuman.eva;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

    public Optional<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getByIdFields(
            String assembly, String contig, long start, VariantType type) {

        IClusteredVariant clusteredVariant = new ClusteredVariant(assembly, 0, contig, start, type, false, null);
        List<AccessionWrapper<IClusteredVariant, String, Long>> variants = this.get(
                Collections.singletonList(clusteredVariant));

        return variants.isEmpty() ? Optional.empty() : Optional.of(new AccessionResponseDTO<>(variants.get(0),
                                                                                              ClusteredVariant::new));
    }
}