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
package uk.ac.ebi.eva.accession.core.model.eva;

import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;

import java.time.LocalDateTime;
import java.util.Objects;

@Document
public class SubmittedVariantEntity extends AccessionedDocument<ISubmittedVariant, Long> implements ISubmittedVariant {

    @Indexed(background = true)
    @Field("seq")
    private String referenceSequenceAccession;

    @Field("tax")
    private int taxonomyAccession;

    @Field("study")
    private String projectAccession;

    private String contig;

    private long start;

    @Field("ref")
    private String referenceAllele;

    @Field("alt")
    private String alternateAllele;

    @Indexed(background = true)
    @Field("rs")
    private Long clusteredVariantAccession;

    @Field("evidence")
    private Boolean supportedByEvidence;

    @Field("asmMatch")
    private Boolean assemblyMatch;

    @Field("allelesMatch")
    private Boolean allelesMatch;

    @Field("validated")
    private Boolean validated;

    private String remappedFrom;

    private LocalDateTime remappedDate;

    private String remappingId;

    private Integer mapWeight;

    protected SubmittedVariantEntity() {
    }

    //Constructor to be used to store remapped submitted variants
    public SubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model, int version,
                                  String remappedFrom, LocalDateTime remappedDate, String remappingId) {
        this(accession, hashedMessage, model, version);
        this.remappedFrom = remappedFrom;
        this.remappedDate = remappedDate;
        this.remappingId = remappingId;
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, ISubmittedVariant model, int version) {
        this(accession, hashedMessage, model.getReferenceSequenceAccession(), model.getTaxonomyAccession(),
             model.getProjectAccession(), model.getContig(), model.getStart(), model.getReferenceAllele(),
             model.getAlternateAllele(), model.getClusteredVariantAccession(), model.isSupportedByEvidence(),
             model.isAssemblyMatch(), model.isAllelesMatch(), model.isValidated(), version);
        this.setCreatedDate(model.getCreatedDate());
    }

    public SubmittedVariantEntity(Long accession, String hashedMessage, String referenceSequenceAccession,
                                  int taxonomyAccession, String projectAccession, String contig, long start,
                                  String referenceAllele, String alternateAllele, Long clusteredVariantAccession,
                                  Boolean supportedByEvidence, Boolean assemblyMatch, Boolean allelesMatch,
                                  Boolean validated, int version) {
        super(hashedMessage, accession, version);
        this.referenceSequenceAccession = referenceSequenceAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
        this.contig = contig;
        this.start = start;
        this.referenceAllele = referenceAllele;
        this.alternateAllele = alternateAllele;
        this.clusteredVariantAccession = clusteredVariantAccession;

        setSupportedByEvidence(supportedByEvidence);
        setAssemblyMatch(assemblyMatch);
        setAllelesMatch(allelesMatch);
        setValidated(validated);
    }

    /**
     * This constructor should only be used when the mapping weight is required
     */
    public SubmittedVariantEntity(Long accession, String hashedMessage, String referenceSequenceAccession,
                                  int taxonomyAccession, String projectAccession, String contig, long start,
                                  String referenceAllele, String alternateAllele, Long clusteredVariantAccession,
                                  Boolean supportedByEvidence, Boolean assemblyMatch, Boolean allelesMatch,
                                  Boolean validated, int version, Integer mapWeight) {
        this(accession, hashedMessage, referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start, 
             referenceAllele, alternateAllele, clusteredVariantAccession, supportedByEvidence, assemblyMatch, 
             allelesMatch, validated, version);
        this.mapWeight = mapWeight == null || mapWeight == 1 ? null : mapWeight;
    }

    public ISubmittedVariant getModel() {
        SubmittedVariant variant = new SubmittedVariant(this);
        variant.setSupportedByEvidence(isSupportedByEvidence());
        variant.setAssemblyMatch(isAssemblyMatch());
        variant.setAllelesMatch(isAllelesMatch());
        variant.setValidated(isValidated());
        variant.setCreatedDate(getCreatedDate());
        variant.setMapWeight(getMapWeight());
        return variant;
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
        return supportedByEvidence == null ? DEFAULT_SUPPORTED_BY_EVIDENCE : supportedByEvidence;
    }

    private void setSupportedByEvidence(Boolean supportedByEvidence) {
        if (supportedByEvidence == null) {
            throw new IllegalArgumentException(
                    "supportedByEvidence should not be null, as null is used for default values");
        } else {
            this.supportedByEvidence = supportedByEvidence == DEFAULT_SUPPORTED_BY_EVIDENCE ? null :
                    supportedByEvidence;
        }
    }

    @Override
    public Boolean isAssemblyMatch() {
        return assemblyMatch == null ? DEFAULT_ASSEMBLY_MATCH : assemblyMatch;
    }

    public void setAssemblyMatch(Boolean assemblyMatch) {
        if (assemblyMatch == null) {
            throw new IllegalArgumentException("assemblyMatch should not be null, as null is used for default values");
        } else {
            this.assemblyMatch = assemblyMatch == DEFAULT_ASSEMBLY_MATCH ? null : assemblyMatch;
        }
    }

    @Override
    public Boolean isAllelesMatch() {
        return allelesMatch == null ? DEFAULT_ALLELES_MATCH : allelesMatch;
    }

    public void setAllelesMatch(Boolean allelesMatch) {
        if (allelesMatch == null) {
            throw new IllegalArgumentException("allelesMatch should not be null, as null is used for default values");
        } else {
            this.allelesMatch = allelesMatch == DEFAULT_ALLELES_MATCH ? null : allelesMatch;
        }
    }

    @Override
    public Boolean isValidated() {
        return validated == null ? DEFAULT_VALIDATED : validated;
    }

    public void setValidated(Boolean validated) {
        if (validated == null) {
            throw new IllegalArgumentException("validated should not be null, as null is used for default values");
        } else {
            this.validated = validated == DEFAULT_VALIDATED ? null : validated;
        }
    }

    public String getRemappedFrom() {
        return remappedFrom;
    }

    public void setRemappedFrom(String remappedFrom) {
        this.remappedFrom = remappedFrom;
    }

    public LocalDateTime getRemappedDate() {
        return remappedDate;
    }

    public void setRemappedDate(LocalDateTime remappedDate) {
        this.remappedDate = remappedDate;
    }

    public String getRemappingId() {
        return remappingId;
    }

    public void setRemappingId(String remappingId) {
        this.remappingId = remappingId;
    }

    public Integer getMapWeight() {
        return mapWeight;
    }

    public void setMapWeight(Integer mapWeight) {
        this.mapWeight = mapWeight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmittedVariantEntity that = (SubmittedVariantEntity) o;
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
                Objects.equals(mapWeight, that.mapWeight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
                            referenceAllele, alternateAllele, clusteredVariantAccession, supportedByEvidence,
                            assemblyMatch, allelesMatch, validated, remappedFrom, remappedDate, mapWeight);
    }

    @Override
    public String toString() {
        return "SubmittedVariantEntity{" +
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
                ", remappedFrom='" + remappedFrom + '\'' +
                ", remappedDate='" + remappedDate + '\'' +
                ", mapWeight=" + mapWeight +
                '}';
    }
}
