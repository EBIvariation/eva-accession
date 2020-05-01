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
package uk.ac.ebi.eva.accession.core.summary;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;

import java.util.function.Function;

/**
 * Creates a string representation from the identifying fields of a SubmittedVariant.
 *
 * This class will usually be used together with {@link uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction}
 * to create a {@link uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService}.
 *
 * Using this summary class and the hashing one is equivalent to the next bash command (replacing the variables):
 * echo -n "assembly_project_contig_start_ref_alt" | sha1sum | awk '{ print toupper($1) }'
 */
public class SubmittedVariantSummaryFunction implements Function<ISubmittedVariant, String> {

    @Override
    public String apply(ISubmittedVariant model) {
        return new StringBuilder()
                .append(model.getReferenceSequenceAccession())
                .append("_").append(model.getProjectAccession())
                .append("_").append(model.getContig())
                .append("_").append(model.getStart())
                .append("_").append(model.getReferenceAllele())
                .append("_").append(model.getAlternateAllele())
                .toString();
    }

}
