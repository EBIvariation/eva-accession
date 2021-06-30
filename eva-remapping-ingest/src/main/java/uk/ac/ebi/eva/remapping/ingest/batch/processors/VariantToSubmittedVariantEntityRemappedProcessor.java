/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.batch.processors;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.time.LocalDateTime;
import java.util.function.Function;

public class VariantToSubmittedVariantEntityRemappedProcessor implements ItemProcessor<Variant,
        SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(VariantToSubmittedVariantEntityRemappedProcessor.class);

    public static final String TAXONOMY_KEY = "TAX";

    public static final String PROJECT_KEY = "PROJECT";

    public static final String RS_KEY = "RS";

    public static final String CREATED_DATE = "CREATED";

    private String assemblyAccession;

    private String remappedFrom;

    private Function<ISubmittedVariant, String> hashingFunction;

    public VariantToSubmittedVariantEntityRemappedProcessor(String assemblyAccession, String remappedFrom) {
        if (assemblyAccession == null || remappedFrom == null) {
            throw new IllegalArgumentException("assembly accession and assembly remapped from must be provided");
        }
        this.assemblyAccession = assemblyAccession;
        this.remappedFrom = remappedFrom;
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public SubmittedVariantEntity process(Variant variant) {
        long accession = Long.parseLong(variant.getMainId().substring(2));
        VariantSourceEntry sourceEntry = variant.getSourceEntries().iterator().next();

        int taxonomyAccession = NumberUtils.createInteger(sourceEntry.getAttribute(TAXONOMY_KEY));
        String projectAccession = sourceEntry.getAttribute(PROJECT_KEY);
        String rsIdTxt = sourceEntry.getAttribute(RS_KEY);
        Long rsId = null;
        if (rsIdTxt != null) {
            if (rsIdTxt.startsWith("rs")) {
                    rsId = NumberUtils.createLong(sourceEntry.getAttribute(RS_KEY).substring(2));
            } else {
                throw new IllegalArgumentException("RS id is not in the correct format: " + rsIdTxt);
            }
        } else {
            logger.warn("Variant {} does not have an RS ID associated: {}", variant.getMainId(), variant);
        }

        SubmittedVariant submittedVariant = new SubmittedVariant(assemblyAccession, taxonomyAccession, projectAccession,
                                                                 variant.getChromosome(), variant.getStart(),
                                                                 variant.getReference(), variant.getAlternate(), rsId);
        String createdDate = sourceEntry.getAttribute(CREATED_DATE);
        submittedVariant.setCreatedDate(LocalDateTime.parse(createdDate));

        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantRemappedEntity = new SubmittedVariantEntity(accession, hash,
                                                                                           submittedVariant, 1,
                                                                                           remappedFrom,
                                                                                           LocalDateTime.now());
        return submittedVariantRemappedEntity;
    }
}
