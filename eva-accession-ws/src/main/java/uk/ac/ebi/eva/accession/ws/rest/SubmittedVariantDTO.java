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
import uk.ac.ebi.eva.accession.core.utils.ISubmittedVariantComparator;

import java.time.LocalDateTime;

public class SubmittedVariantDTO implements ISubmittedVariant {

    private String assemblyAccession;

    private int taxonomyAccession;

    private String projectAccession;

    private String contig;

    private long start;

    private String referenceAllele;

    private String alternateAllele;

    private Long clusteredVariantAccession;

    private Boolean supportedByEvidence;

    private Boolean assemblyMatch;

    private Boolean allelesMatch;

    private Boolean validated;

    private LocalDateTime createdDate;

    SubmittedVariantDTO() {
    }

    public SubmittedVariantDTO(ISubmittedVariant model) {
        this(model.getAssemblyAccession(), model.getTaxonomyAccession(), model.getProjectAccession(), model.getContig(),
             model.getStart(), model.getReferenceAllele(), model.getAlternateAllele(),
             model.getClusteredVariantAccession(), model.isSupportedByEvidence(), model.isAssemblyMatch(),
             model.isAllelesMatch(), model.isValidated(), model.getCreatedDate());
    }

    public SubmittedVariantDTO(String assemblyAccession, int taxonomyAccession, String projectAccession,
                               String contig, long start, String referenceAllele, String alternateAllele,
                               Long clusteredVariantAccession, Boolean supportedByEvidence, Boolean assemblyMatch,
                               Boolean allelesMatch, Boolean validated, LocalDateTime createdDate) {
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.clusteredVariantAccession = clusteredVariantAccession;
        this.supportedByEvidence = supportedByEvidence;
        this.assemblyMatch = assemblyMatch;
        this.allelesMatch = allelesMatch;
        this.validated = validated;
        this.createdDate = createdDate;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
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
    public Long getClusteredVariantAccession() {
        return clusteredVariantAccession;
    }

    @Override
    public Boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

    @Override
    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ISubmittedVariant)) {
            return false;
        }

        return ISubmittedVariantComparator.equals(this, (ISubmittedVariant) o);
    }

    @Override
    public int hashCode() {
        return ISubmittedVariantComparator.hashCode(this);
    }
}
