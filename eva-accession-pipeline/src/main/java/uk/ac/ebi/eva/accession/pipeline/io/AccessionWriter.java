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

import org.springframework.batch.item.ItemWriter;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;

import java.util.List;
import java.util.Map;

public class AccessionWriter implements ItemWriter<ISubmittedVariant> {

    private SubmittedVariantAccessioningService service;

    private AccessionSummaryWriter accessionSummaryWriter;

    public AccessionWriter(SubmittedVariantAccessioningService service, AccessionSummaryWriter accessionSummaryWriter) {
        this.service = service;
        this.accessionSummaryWriter = accessionSummaryWriter;
    }

    @Override
    public void write(List<? extends ISubmittedVariant> variants) throws Exception {
        Map<Long, ISubmittedVariant> accessions = service.getOrCreateAccessions(variants);
        accessionSummaryWriter.write(accessions);
    }
}
