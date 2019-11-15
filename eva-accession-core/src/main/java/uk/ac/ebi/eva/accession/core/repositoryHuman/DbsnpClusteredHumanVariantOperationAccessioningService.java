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
package uk.ac.ebi.eva.accession.core.repositoryHuman;

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;

import java.util.ArrayList;
import java.util.List;

public class DbsnpClusteredHumanVariantOperationAccessioningService {

    private final DbsnpClusteredHumanVariantOperationRepository operationAccessionRepository;

    public DbsnpClusteredHumanVariantOperationAccessioningService(DbsnpClusteredHumanVariantOperationRepository operationAccessionRepository) {
        this.operationAccessionRepository = operationAccessionRepository;
    }

    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getByAccession(Long identifier) {
        List<DbsnpClusteredVariantOperationEntity> operations = operationAccessionRepository.findAllByAccession(identifier);

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                new ArrayList<>();
        for (DbsnpClusteredVariantOperationEntity operation : operations) {
            List<DbsnpClusteredVariantInactiveEntity> inactiveEntities = operation.getInactiveObjects();
            for (DbsnpClusteredVariantInactiveEntity inactiveEntity : inactiveEntities) {
                AccessionWrapper<IClusteredVariant, String, Long> wrapper =
                        new AccessionWrapper(identifier, operation.getId(), inactiveEntity.getModel());
                clusteredVariants.add(new AccessionResponseDTO<>(wrapper, ClusteredVariant::new));
            }
        }

        return clusteredVariants;
    }
}
