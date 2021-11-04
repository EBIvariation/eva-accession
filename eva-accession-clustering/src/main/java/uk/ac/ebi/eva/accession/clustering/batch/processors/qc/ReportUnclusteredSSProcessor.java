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
package uk.ac.ebi.eva.accession.clustering.batch.processors.qc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.Objects;

public class ReportUnclusteredSSProcessor implements ItemProcessor<SubmittedVariantEntity, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ReportUnclusteredSSProcessor.class);

    @Override
    public SubmittedVariantEntity process(SubmittedVariantEntity submittedVariantEntity) {
           if (submittedVariantEntity.isAssemblyMatch() && Objects.isNull(
                   submittedVariantEntity.getClusteredVariantAccession())) {
               logger.error("SS record {} in assembly {} was not clustered",
                            submittedVariantEntity, submittedVariantEntity.getReferenceSequenceAccession());
               return null;
           }
           return submittedVariantEntity;
    }
}
