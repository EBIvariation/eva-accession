/*
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
 */
package uk.ac.ebi.eva.accession.clustering.batch.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ClusteringVariantProcessor implements ItemProcessor<List<SubmittedVariant>, List<SubmittedVariant>> {

    private List<Long> accessions;

    private Iterator<Long> iterator;

    private Map<String, Long> assignedAccessions;

    private Function<IClusteredVariant, String> hashingFunction;

    ClusteringVariantProcessor() {
        initializeAccessions();
        this.iterator = this.accessions.iterator();
        this.assignedAccessions = new HashMap<>();
        hashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    private void initializeAccessions() {
        this.accessions = new ArrayList<>();
        long firstAccession = 1L;
        long lastAccession = 10L;
        for (long i = firstAccession; i <= lastAccession; i++) {
            accessions.add(i);
        }
    }

    @Override
    public List<SubmittedVariant> process(List<SubmittedVariant> submittedVariants) {
        for (SubmittedVariant submittedVariant : submittedVariants) {
            ClusteredVariant clusteredVariant = new ClusteredVariant(submittedVariant.getProjectAccession(),
                                                                     submittedVariant.getTaxonomyAccession(),
                                                                     submittedVariant.getContig(),
                                                                     submittedVariant.getStart(),
                                                                     getVariantType(
                                                                             submittedVariant.getReferenceAllele(),
                                                                             submittedVariant.getAlternateAllele()),
                                                                     submittedVariant.isValidated(),
                                                                     submittedVariant.getCreatedDate());
            String hash = hashingFunction.apply(clusteredVariant);
            if (assignedAccessions.containsKey(hash)) {
                Long existingClusteredVariantAccession = assignedAccessions.get(hash);
                submittedVariant.setClusteredVariantAccession(existingClusteredVariantAccession);
            } else {
                Long generatedClusteredVariantAccession = iterator.next();
                submittedVariant.setClusteredVariantAccession(generatedClusteredVariantAccession);
                assignedAccessions.putIfAbsent(hash, generatedClusteredVariantAccession);
            }
        }
        return submittedVariants;
    }

    private VariantType getVariantType(String reference, String alternate) {
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate, 0);
        return variantType;
    }
}
