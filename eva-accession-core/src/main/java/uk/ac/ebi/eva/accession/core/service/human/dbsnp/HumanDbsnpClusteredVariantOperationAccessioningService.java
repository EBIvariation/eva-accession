/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.service.human.dbsnp;

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class HumanDbsnpClusteredVariantOperationAccessioningService {

    private final HumanDbsnpClusteredVariantOperationRepository operationAccessionRepository;

    private static Function<IClusteredVariant, String> hashingFunctionClustered =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public HumanDbsnpClusteredVariantOperationAccessioningService(
            HumanDbsnpClusteredVariantOperationRepository operationAccessionRepository) {
        this.operationAccessionRepository = operationAccessionRepository;
    }

    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getByAccession(Long identifier) {
        List<DbsnpClusteredVariantOperationEntity> operations = operationAccessionRepository.findAllByAccession(identifier);
        return getAccessionResponseDTOS(operations);
    }

    private List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getAccessionResponseDTOS(
            List<DbsnpClusteredVariantOperationEntity> operations) {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                new ArrayList<>();
        for (DbsnpClusteredVariantOperationEntity operation : operations) {
            List<DbsnpClusteredVariantInactiveEntity> inactiveEntities = operation.getInactiveObjects();
            for (DbsnpClusteredVariantInactiveEntity inactiveEntity : inactiveEntities) {
                AccessionWrapper<IClusteredVariant, String, Long> wrapper =
                        new AccessionWrapper<>(operation.getAccession(), operation.getId(), inactiveEntity.getModel());
                clusteredVariants.add(new AccessionResponseDTO<>(wrapper, ClusteredVariant::new));
            }
        }
        return clusteredVariants;
    }

    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getOriginalVariant(
            IClusteredVariant clusteredVariant) {
        String hash =  hashingFunctionClustered.apply(clusteredVariant);
        List<DbsnpClusteredVariantOperationEntity> clusteredVariants = operationAccessionRepository.
                findAllByInactiveObjects_HashedMessage(hash);
        return getAccessionResponseDTOS(clusteredVariants);
    }
}
