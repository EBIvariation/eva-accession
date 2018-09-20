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
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
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
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/v1/clustered-variants")
@Api(tags = {"Clustered variants"})
public class ClusteredVariantsRestController {

    private final BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController;

    private SubmittedVariantAccessioningService submittedVariantsService;

    public ClusteredVariantsRestController(
            BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController,
            SubmittedVariantAccessioningService submittedVariantsService) {
        this.basicRestController = basicRestController;
        this.submittedVariantsService = submittedVariantsService;
    }

    @ApiOperation(value = "Find clustered variants (RS) by identifier", notes = "This endpoint returns an accessioned" +
            " clustered variant (RS). See " +
            "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#clustered-variant-refsnp-or-rs " +
            "for an explanation of each field.")
    @GetMapping(value = "/{identifiers}", produces = "application/json")
    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> get(
            @PathVariable
            @ApiParam(value = "List of numerical identifiers of clustered variants, e.g.: 3000000000,3000000002",
                    required = true)
                    List<Long> identifiers) {
        return basicRestController.get(identifiers);
    }

    @ApiOperation(value = "Find submitted variants (SS) by clustered variant identifier (RS)", notes = "This endpoint"
            + " takes clustered variant identifiers (RS) and returns all accessioned submitted variants (SS) linked "
            + "to the former. See "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant-subsnp-or-ss "
            + "for an explanation of each field.")
    @GetMapping(value = "/{identifiers}/submitted", produces = "application/json")
    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getSubmittedVariants(
            @PathVariable
            @ApiParam(value = "List of numerical identifiers of clustered variants, e.g.: 869808637", required = true)
                    List<Long> identifiers) {

        List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants = submittedVariantsService
                .getByClusteredVariantAccessionIn(
                identifiers);
        return submittedVariants
                .stream()
                .map(wrapper -> new AccessionResponseDTO<>(wrapper, SubmittedVariant::new))
                .collect(Collectors.toList());
    }
}

