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
package uk.ac.ebi.eva.accession.core.model;

import java.time.LocalDateTime;
import java.util.Objects;

public class SubmittedVariant implements ISubmittedVariant {

    private String referenceSequenceAccession;

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

    private Integer mapWeight;

    private LocalDateTime createdDate;

    private String remappedFrom;

    private LocalDateTime remappedDate;

    private String remappingId;

    private Long backPropagatedVariantAccession;

    SubmittedVariant() {
    }

    /**
     * This constructor sets the flags (supportedByEvidence, assemblyMatch, allelesMatch, validated) to its default
     * value defined in {@link ISubmittedVariant}
     *
     * Important note: do not use this constructor for objects that will be stored in the accessioning database, as it
     * would write the flags with an arbitrary value. This constructor is intended to create objects for querying only,
     * where the flags are ignored.
     */
    public SubmittedVariant(String referenceSequenceAccession, int taxonomyAccession, String projectAccession,
                            String contig, long start, String referenceAllele, String alternateAllele,
                            Long clusteredVariantAccession) {
        this(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start, referenceAllele, alternateAllele,
             clusteredVariantAccession, DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH, DEFAULT_ALLELES_MATCH,
             DEFAULT_VALIDATED, null);
    }

    public SubmittedVariant(ISubmittedVariant variant) {
        this(variant.getReferenceSequenceAccession(), variant.getTaxonomyAccession(), variant.getProjectAccession(),
             variant.getContig(), variant.getStart(), variant.getReferenceAllele(), variant.getAlternateAllele(),
             variant.getClusteredVariantAccession(), variant.isSupportedByEvidence(), variant.isAssemblyMatch(),
             variant.isAllelesMatch(), variant.isValidated(), variant.getCreatedDate());
        this.setMapWeight(variant.getMapWeight());
        this.setRemappedFrom(variant.getRemappedFrom());
        this.setRemappedDate(variant.getRemappedDate());
        this.setRemappingId(variant.getRemappingId());
        this.setBackPropagatedVariantAccession(variant.getBackPropagatedVariantAccession());
    }

    /**
     * This is the constructor that has to be used to instantiate objects that will be written in the accessioning
     * database.
     */
    public SubmittedVariant(String referenceSequenceAccession, int taxonomyAccession, String projectAccession,
                            String contig, long start, String referenceAllele, String alternateAllele,
                            Long clusteredVariantAccession, Boolean supportedByEvidence, Boolean assemblyMatch,
                            Boolean allelesMatch, Boolean validated, LocalDateTime createdDate) {
        if(Objects.isNull(referenceSequenceAccession)) {
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

        this.referenceSequenceAccession = referenceSequenceAccession;
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
    public String getReferenceSequenceAccession() {
        return referenceSequenceAccession;
    }

    public void setReferenceSequenceAccession(String referenceSequenceAccession) {
        this.referenceSequenceAccession = referenceSequenceAccession;
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
    public Integer getMapWeight() {
        return mapWeight;
    }

    public void setMapWeight(Integer mapWeight) {
        this.mapWeight = mapWeight;
    }

    @Override
    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
    }

    @Override
    public String getRemappedFrom() {
        return remappedFrom;
    }

    public void setRemappedFrom(String remappedFrom) {
        this.remappedFrom = remappedFrom;
    }

    @Override
    public LocalDateTime getRemappedDate() {
        return remappedDate;
    }

    public void setRemappedDate(LocalDateTime remappedDate) {
        this.remappedDate = remappedDate;
    }

    @Override
    public String getRemappingId() {
        return remappingId;
    }

    @Override
    public Long getBackPropagatedVariantAccession() {
        return backPropagatedVariantAccession;
    }

    public void setBackPropagatedVariantAccession(Long backPropagatedVariantAccession) {
        this.backPropagatedVariantAccession = backPropagatedVariantAccession;
    }

    public void setRemappingId(String remappingId) {
        this.remappingId = remappingId;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmittedVariant that = (SubmittedVariant) o;
        return taxonomyAccession == that.taxonomyAccession &&
                start == that.start &&
                Objects.equals(referenceSequenceAccession, that.referenceSequenceAccession) &&
                Objects.equals(projectAccession, that.projectAccession) &&
                Objects.equals(contig, that.contig) &&
                Objects.equals(referenceAllele, that.referenceAllele) &&
                Objects.equals(alternateAllele, that.alternateAllele) &&
                Objects.equals(clusteredVariantAccession, that.clusteredVariantAccession) &&
                Objects.equals(supportedByEvidence, that.supportedByEvidence) &&
                Objects.equals(assemblyMatch, that.assemblyMatch) &&
                Objects.equals(allelesMatch, that.allelesMatch) &&
                Objects.equals(validated, that.validated) &&
                Objects.equals(remappedFrom, that.remappedFrom) &&
                Objects.equals(remappedDate, that.remappedDate) &&
                Objects.equals(remappingId, that.remappingId) &&
                Objects.equals(backPropagatedVariantAccession, that.backPropagatedVariantAccession);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
                referenceAllele, alternateAllele, clusteredVariantAccession, supportedByEvidence,
                assemblyMatch, allelesMatch, validated, remappedFrom, remappedDate, remappingId,
                backPropagatedVariantAccession);
    }

    @Override
    public String toString() {
        return "SubmittedVariant{" +
                "referenceSequenceAccession='" + referenceSequenceAccession + '\'' +
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
                ", mapWeight=" + mapWeight +
                ", createdDate=" + createdDate +
                ", remappedFrom=" + remappedFrom +
                ", remappedDate=" + remappedDate +
                ", remappingId=" + remappingId +
                ", backPropagatedVariantAccession=" + backPropagatedVariantAccession +
                '}';
    }
}
