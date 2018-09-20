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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import java.util.List;

@RestController
@RequestMapping(value = "/v1/submitted-variants")
@Api(tags = {"Submitted variants"})
public class SubmittedVariantsRestController {

    private final BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicRestController;

    public SubmittedVariantsRestController(
            BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicRestController) {
        this.basicRestController = basicRestController;
    }

    @ApiOperation(value = "Find submitted variants (SS) by identifier", notes = "This endpoint returns accessioned"
            + " submitted variants (SS). See " +
            "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant-subsnp-or-ss " +
            "for an explanation of each field.")
    @GetMapping(value = "/{identifiers}", produces = "application/json")
    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> get(
            @PathVariable
            @ApiParam(value = "List of numerical identifiers of submitted variants, e.g.: 5000000000,5000000002",
                    required = true)
                    List<Long> identifiers) {
        return basicRestController.get(identifiers);
    }
}

