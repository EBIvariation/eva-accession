/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.ws.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/v1/clustered-variants")
@Api(tags = {"Clustered variants"})
public class ClusteredVariantsRestController {

    private final BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController;

    private DbsnpSubmittedVariantAccessioningRepository submittedVariantsRepository;

    private DbsnpSubmittedVariantSummaryFunction submittedVariantHashFunction;

    public ClusteredVariantsRestController(
            BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController,
            DbsnpSubmittedVariantAccessioningRepository submittedVariantsRepository) {
        this.basicRestController = basicRestController;
        this.submittedVariantsRepository = submittedVariantsRepository;
        submittedVariantHashFunction = new DbsnpSubmittedVariantSummaryFunction();
    }

    @ApiOperation(value = "Find clustered variants by identifier")
    @GetMapping(value = "/{identifiers}", produces = "application/json")
    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> get(
            @PathVariable List<Long> identifiers) {
        return basicRestController.get(identifiers);
    }

    @ApiOperation(value = "Find submitted variants by clustered variant identifier")
    @GetMapping(value = "/{identifiers}/submitted", produces = "application/json")
    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getSubmittedVariants(
            @PathVariable List<Long> identifiers) {
        List<DbsnpSubmittedVariantEntity> submittedVariantEntities = submittedVariantsRepository
                .findByClusteredVariantAccessionIn(identifiers);

        // return retrieved SubmittedVariant objects encapsulated in AccessionResponseDTO objects
        return submittedVariantEntities.stream().map(this::submittedVariantToAccessionWrapper).map(
                this::acessionWrapperToResponseDTO).collect(Collectors.toList());
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> submittedVariantToAccessionWrapper(
            DbsnpSubmittedVariantEntity submittedVariant) {
        return new AccessionWrapper<>(submittedVariant.getAccession(),
                                      submittedVariantHashFunction.apply(submittedVariant),
                                      submittedVariant.getModel(),
                                      submittedVariant.getVersion());
    }

    private AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long> acessionWrapperToResponseDTO(
            AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper) {
        return new AccessionResponseDTO<>(accessionWrapper, SubmittedVariant::new);
    }
}

