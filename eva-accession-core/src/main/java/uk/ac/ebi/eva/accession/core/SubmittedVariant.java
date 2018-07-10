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
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.eva.accession.core.utils.ISubmittedVariantComparator;

import java.time.LocalDateTime;
import java.util.Objects;

public class SubmittedVariant implements ISubmittedVariant {

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

    public SubmittedVariant(String assemblyAccession, int taxonomyAccession, String projectAccession, String contig,
                            long start, String referenceAllele, String alternateAllele, Long clusteredVariantAccession,
                            Boolean supportedByEvidence, Boolean assemblyMatch, Boolean allelesMatch,
                            Boolean validated) {
        if(Objects.isNull(assemblyAccession)) {
            throw new IllegalArgumentException("Assembly accession is required");
        }
        if(Objects.isNull(projectAccession)) {
            throw new IllegalArgumentException("Project accession is required");
        }
        if(Objects.isNull(contig)) {
            throw new IllegalArgumentException("Contig is required");
        }
        if(Objects.isNull(referenceAllele)) {
            throw new IllegalArgumentException("Reference allele is required");
        }
        if(Objects.isNull(alternateAllele)) {
            throw new IllegalArgumentException("Alternate allele is required");
        }

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
        this.createdDate = null;
    }

    @Override
    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    @Override
    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public void setTaxonomyAccession(int taxonomyAccession) {
        this.taxonomyAccession = taxonomyAccession;
    }

    @Override
    public String getProjectAccession() {
        return projectAccession;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    @Override
    public String getContig() {
        return contig;
    }

    public void setContig(String contig) {
        this.contig = contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @Override
    public String getReferenceAllele() {
        return referenceAllele;
    }

    public void setReferenceAllele(String referenceAllele) {
        this.referenceAllele = referenceAllele;
    }

    @Override
    public String getAlternateAllele() {
        return alternateAllele;
    }

    public void setAlternateAllele(String alternateAllele) {
        this.alternateAllele = alternateAllele;
    }

    @Override
    public Long getClusteredVariantAccession() {
        return clusteredVariantAccession;
    }

    public void setClusteredVariantAccession(Long clusteredVariantAccession) {
        this.clusteredVariantAccession = clusteredVariantAccession;
    }

    @Override
    public Boolean isSupportedByEvidence() {
        return supportedByEvidence;
    }

    public void setSupportedByEvidence(boolean supportedByEvidence) {
        this.supportedByEvidence = supportedByEvidence;
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch;
    }

    public void setAssemblyMatch(Boolean assemblyMatch) {
        this.assemblyMatch = assemblyMatch;
    }

    @Override
    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch;
    }

    public void setAllelesMatch(Boolean allelesMatch) {
        this.allelesMatch = allelesMatch;
    }

    @Override
    public Boolean isValidated() {
        return validated;
    }

    public void setValidated(Boolean validated) {
        this.validated = validated;
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

    @Override
    public String toString() {
        return "SubmittedVariant{" +
                "assemblyAccession='" + assemblyAccession + '\'' +
                ", taxonomyAccession=" + taxonomyAccession +
                ", projectAccession='" + projectAccession + '\'' +
                ", contig='" + contig + '\'' +
                ", start=" + start +
                ", referenceAllele='" + referenceAllele + '\'' +
                ", alternateAllele='" + alternateAllele + '\'' +
                ", clusteredVariantAccession=" + clusteredVariantAccession +
                ", supportedByEvidence=" + supportedByEvidence +
                ", assemblyMatch=" + assemblyMatch +
                ", allelesMatch=" + allelesMatch +
                ", validated=" + validated +
                ", createdDate=" + createdDate +
                '}';
    }
}
