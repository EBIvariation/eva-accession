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

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;

import java.util.List;

public class AccessionWriter implements ItemStreamWriter<ISubmittedVariant> {

    private SubmittedVariantAccessioningService service;

    private AccessionReportWriter accessionReportWriter;

    public AccessionWriter(SubmittedVariantAccessioningService service, AccessionReportWriter accessionReportWriter) {
        this.service = service;
        this.accessionReportWriter = accessionReportWriter;
    }

    @Override
    public void write(List<? extends ISubmittedVariant> variants) throws Exception {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getOrCreateAccessions(variants);
        accessionReportWriter.write(accessions);
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
