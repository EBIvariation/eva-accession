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
    public SubmittedVariant(String assemblyAccession, int taxonomyAccession, String projectAccession,
                            String contig, long start, String referenceAllele, String alternateAllele,
                            Long clusteredVariantAccession) {
        this(assemblyAccession, taxonomyAccession, projectAccession, contig, start, referenceAllele, alternateAllele,
             clusteredVariantAccession, DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH, DEFAULT_ALLELES_MATCH,
             DEFAULT_VALIDATED);
    }

    public SubmittedVariant(ISubmittedVariant variant) {
        this(variant.getAssemblyAccession(), variant.getTaxonomyAccession(), variant.getProjectAccession(),
             variant.getContig(), variant.getStart(), variant.getReferenceAllele(), variant.getAlternateAllele(),
             variant.getClusteredVariantAccession(), variant.isSupportedByEvidence(), variant.isAssemblyMatch(),
             variant.isAllelesMatch(), variant.isValidated());
    }

    /**
     * This is the constructor that has to be used to instantiate objects that will be written in the accessioning
     * database.
     */
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
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubmittedVariant)) {
            return false;
        }

        SubmittedVariant variant = (SubmittedVariant) o;

        if (taxonomyAccession != variant.taxonomyAccession) {
            return false;
        }
        if (start != variant.start) {
            return false;
        }
        if (!assemblyAccession.equals(variant.assemblyAccession)) {
            return false;
        }
        if (!projectAccession.equals(variant.projectAccession)) {
            return false;
        }
        if (!contig.equals(variant.contig)) {
            return false;
        }
        if (!referenceAllele.equals(variant.referenceAllele)) {
            return false;
        }
        if (!alternateAllele.equals(variant.alternateAllele)) {
            return false;
        }
        if (clusteredVariantAccession != null ? !clusteredVariantAccession.equals(
                variant.clusteredVariantAccession) : variant.clusteredVariantAccession != null) {
            return false;
        }
        if (supportedByEvidence != null ? !supportedByEvidence.equals(
                variant.supportedByEvidence) : variant.supportedByEvidence != null) {
            return false;
        }
        if (assemblyMatch != null ? !assemblyMatch.equals(variant.assemblyMatch) : variant.assemblyMatch != null) {
            return false;
        }
        if (allelesMatch != null ? !allelesMatch.equals(variant.allelesMatch) : variant.allelesMatch != null) {
            return false;
        }
        if (validated != null ? !validated.equals(variant.validated) : variant.validated != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = assemblyAccession.hashCode();
        result = 31 * result + taxonomyAccession;
        result = 31 * result + projectAccession.hashCode();
        result = 31 * result + contig.hashCode();
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + referenceAllele.hashCode();
        result = 31 * result + alternateAllele.hashCode();
        result = 31 * result + (clusteredVariantAccession != null ? clusteredVariantAccession.hashCode() : 0);
        result = 31 * result + (supportedByEvidence != null ? supportedByEvidence.hashCode() : 0);
        result = 31 * result + (assemblyMatch != null ? assemblyMatch.hashCode() : 0);
        result = 31 * result + (allelesMatch != null ? allelesMatch.hashCode() : 0);
        result = 31 * result + (validated != null ? validated.hashCode() : 0);
        return result;
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
