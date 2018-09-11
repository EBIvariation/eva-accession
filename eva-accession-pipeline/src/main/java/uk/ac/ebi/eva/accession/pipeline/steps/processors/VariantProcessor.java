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
package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;

public class VariantProcessor implements ItemProcessor<IVariant, ISubmittedVariant> {

    private static final Long UNDEFINED_CLUSTERED_VARIANT = null;

    // assumed by this pipeline, independent of the default in ISubmittedVariant
    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    // assumed by this pipeline, independent of the default in ISubmittedVariant
    private static final Boolean MATCHES_ASSEMBLY = true;

    // can only be false in eva-accession-import, independent of the default in ISubmittedVariant
    private static final Boolean ALLELES_MATCH = true;

    // assumed by this pipeline, independent of the default in ISubmittedVariant
    private static final Boolean VALIDATED = false;

    private String assemblyAccession;

    private int taxonomyAccession;

    private String projectAccession;

    public VariantProcessor(String assemblyAccession, int taxonomyAccession, String projectAccession) {
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
    }

    @Override
    public ISubmittedVariant process(final IVariant variant) throws Exception {
        return new SubmittedVariant(assemblyAccession, taxonomyAccession, projectAccession, variant.getChromosome(),
                                    variant.getStart(), variant.getReference(), variant.getAlternate(),
                                    UNDEFINED_CLUSTERED_VARIANT,
                                    SUPPORTED_BY_EVIDENCE,
                                    MATCHES_ASSEMBLY,
                                    ALLELES_MATCH,
                                    VALIDATED, null);
    }
}
