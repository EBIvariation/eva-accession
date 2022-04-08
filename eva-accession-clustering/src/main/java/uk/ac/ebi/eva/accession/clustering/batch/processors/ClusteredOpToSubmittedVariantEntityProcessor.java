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
import org.springframework.lang.NonNull;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import java.util.List;
import java.util.stream.Collectors;

public class ClusteredOpToSubmittedVariantEntityProcessor implements
        ItemProcessor<ClusteredVariantOperationEntity, List<SubmittedVariantEntity>> {

    private final String assemblyAccession;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    public ClusteredOpToSubmittedVariantEntityProcessor(String assemblyAccession,
                                                        SubmittedVariantAccessioningService
                                                                submittedVariantAccessioningService) {
        if (assemblyAccession == null) {
            throw new IllegalArgumentException("assembly accession must be provided when reading from a VCF");
        }
        this.assemblyAccession = assemblyAccession;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
    }

    @Override
    public List<SubmittedVariantEntity> process(@NonNull ClusteredVariantOperationEntity
                                                            clusteredVariantOperationEntity) {
        List<Long> ssToFindInOriginalAssembly =
                clusteredVariantOperationEntity.getInactiveObjects().stream()
                                               .map(InactiveSubDocument::getAccession).collect(Collectors.toList());
        return this.submittedVariantAccessioningService.getAllActiveByAssemblyAndAccessionIn(
                           this.assemblyAccession,
                           ssToFindInOriginalAssembly).stream().map(e -> new SubmittedVariantEntity(e.getAccession(),
                                                                                             e.getHash(),
                                                                                             e.getData(),
                                                                                             e.getVersion()))
                                                       .collect(Collectors.toList());
    }
}
