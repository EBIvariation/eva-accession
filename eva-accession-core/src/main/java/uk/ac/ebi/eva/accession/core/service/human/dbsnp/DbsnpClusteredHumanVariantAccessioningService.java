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

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class DbsnpClusteredHumanVariantAccessioningService {

    private final ClusteredVariantAccessioningService humanService;

    private final DbsnpClusteredHumanVariantOperationAccessioningService operationsService;

    public DbsnpClusteredHumanVariantAccessioningService(
            @Qualifier("humanActiveService") ClusteredVariantAccessioningService humanService,
            @Qualifier("humanOperationsService") DbsnpClusteredHumanVariantOperationAccessioningService operationsService) {
        this.humanService = humanService;
        this.operationsService = operationsService;
    }

    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getAllByAccession(
            Long identifier) {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                new ArrayList<>();
        clusteredVariants.addAll(getHumanClusteredVariants(identifier));
        clusteredVariants.addAll(operationsService.getByAccession(identifier));
        return clusteredVariants;
    }

    private List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getHumanClusteredVariants(
            Long identifier) {
        try {
            AccessionWrapper<IClusteredVariant, String, Long> wrapper = humanService.getByAccession(identifier);
            return Collections.singletonList(new AccessionResponseDTO<>(wrapper, ClusteredVariant::new));
        } catch (AccessionDoesNotExistException | AccessionMergedException | AccessionDeprecatedException e) {
            return Collections.emptyList();
        }
    }

    public Optional<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getByIdFields(
            String assembly, String contig, long start, VariantType type) {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                new ArrayList<>();

        IClusteredVariant clusteredVariantToSearch = new ClusteredVariant(assembly, 0, contig, start, type, false,
                                                                          null);

        List<AccessionWrapper<IClusteredVariant, String, Long>> activeClusteredVariantWrappers =
                humanService.get(Collections.singletonList(clusteredVariantToSearch));
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> activeClusteredVariants =
                activeClusteredVariantWrappers.stream()
                                              .map(v -> new AccessionResponseDTO<>(v, ClusteredVariant::new))
                                              .collect(Collectors.toList());
        clusteredVariants.addAll(activeClusteredVariants);

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> activeClusteredVariantsInOperations =
                operationsService.getOriginalVariant(clusteredVariantToSearch);
        clusteredVariants.addAll(activeClusteredVariantsInOperations);

        return clusteredVariants.isEmpty() ? Optional.empty() : Optional.of(clusteredVariants);
    }

}
