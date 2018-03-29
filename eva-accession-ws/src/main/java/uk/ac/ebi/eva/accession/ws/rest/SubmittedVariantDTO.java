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

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

public class SubmittedVariantDTO implements ISubmittedVariant {

    private String assemblyAccession;

    private String taxonomyAccession;

    private String projectAccession;

    private String contig;

    private long start;

    private String referenceAllele;

    private String alternateAllele;

    private boolean supportedByEvidence;

    SubmittedVariantDTO() {
    }

    public SubmittedVariantDTO(ISubmittedVariant model) {
        this(model.getAssemblyAccession(), model.getTaxonomyAccession(), model.getProjectAccession(), model.getContig(),
             model.getStart(), model.getReferenceAllele(), model.getAlternateAllele(), model.isSupportedByEvidence());
    }

    public SubmittedVariantDTO(String assemblyAccession, String taxonomyAccession, String projectAccession,
                               String contig, long start, String referenceAllele, String alternateAllele,
                               boolean supportedByEvidence) {
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.supportedByEvidence = supportedByEvidence;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    @Override
    public String getTaxonomyAccession() {
        return taxonomyAccession;
    }

    @Override
    public String getProjectAccession() {
        return projectAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public String getReferenceAllele() {
        return referenceAllele;
    }

    @Override
    public String getAlternateAllele() {
        return alternateAllele;
    }

    @Override
    public boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }
}
