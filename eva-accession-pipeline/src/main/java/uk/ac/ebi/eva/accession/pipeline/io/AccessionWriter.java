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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.AccessionWrapperComparator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AccessionWriter implements ItemStreamWriter<ISubmittedVariant> {

    private static final Logger logger = LoggerFactory.getLogger(AccessionWriter.class);

    private SubmittedVariantAccessioningService service;

    private AccessionReportWriter accessionReportWriter;

    public AccessionWriter(SubmittedVariantAccessioningService service, AccessionReportWriter accessionReportWriter) {
        this.service = service;
        this.accessionReportWriter = accessionReportWriter;
    }

    @Override
    public void write(List<? extends ISubmittedVariant> variants) throws Exception {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getOrCreate(variants);
        accessions.sort(new AccessionWrapperComparator(variants));
        accessionReportWriter.write(accessions);
        checkCountsMatch(variants, accessions);
    }

    void checkCountsMatch(List<? extends ISubmittedVariant> variants,
                          List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions) {
        if (variants.size() != accessions.size()) {
            Set<ISubmittedVariant> accessionedVariants = accessions.stream()
                                                                   .map(AccessionWrapper::getData)
                                                                   .collect(Collectors.toSet());
            HashSet<ISubmittedVariant> distinctVariants = new HashSet<>(variants);
            int duplicateCount = variants.size() - distinctVariants.size();
            if (duplicateCount != 0) {
                logger.warn("A variant chunk contains {} repeated variants. This is not an error, but please check " +
                                    "it's expected.", duplicateCount);
            }

            List<ISubmittedVariant> variantsWithoutAccession = distinctVariants.stream()
                                                                               .filter(v -> !accessionedVariants.contains(v))
                                                                               .collect(Collectors.toList());
            if (variantsWithoutAccession.size() != 0) {
                logger.error("A problem occurred while accessioning a chunk. Total num variants = {}, distinct = {}, " +
                                     "duplicate = {}, accessioned = {}, not accessioned = {}",
                             variants.size(), distinctVariants.size(), duplicateCount, accessionedVariants.size(),
                             variantsWithoutAccession.size());
                logger.error("The non-accessioned variants are: {}", variantsWithoutAccession.toString());

                throw new IllegalStateException(
                        "A problem occurred while accessioning a chunk. See log for details.");
            }
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        accessionReportWriter.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        accessionReportWriter.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        accessionReportWriter.close();
    }
}
