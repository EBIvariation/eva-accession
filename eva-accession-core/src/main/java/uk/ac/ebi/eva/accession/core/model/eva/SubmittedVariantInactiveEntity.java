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
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;

import java.time.LocalDateTime;

@Document
public class SubmittedVariantInactiveEntity extends InactiveSubDocument<ISubmittedVariant, Long>
        implements ISubmittedVariant {

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

    private Integer mapWeight;

    private String remappedFrom;

    private LocalDateTime remappedDate;

    private String remappingId;

    private Long backPropagatedVariantAccession;

    public SubmittedVariantInactiveEntity() {
    }

    public SubmittedVariantInactiveEntity(SubmittedVariantEntity submittedVariantEntity) {
        super(submittedVariantEntity);
        this.referenceSequenceAccession = submittedVariantEntity.getReferenceSequenceAccession();
        this.taxonomyAccession = submittedVariantEntity.getTaxonomyAccession();
        this.projectAccession = submittedVariantEntity.getProjectAccession();
        this.contig = submittedVariantEntity.getContig();
        this.start = submittedVariantEntity.getStart();
        this.referenceAllele = submittedVariantEntity.getReferenceAllele();
        this.alternateAllele = submittedVariantEntity.getAlternateAllele();
        this.clusteredVariantAccession = submittedVariantEntity.getClusteredVariantAccession();
        this.supportedByEvidence = submittedVariantEntity.isSupportedByEvidence();
        this.assemblyMatch = submittedVariantEntity.isAssemblyMatch();
        this.allelesMatch = submittedVariantEntity.isAllelesMatch();
        this.validated = submittedVariantEntity.isValidated();
        this.mapWeight = submittedVariantEntity.getMapWeight();
        this.remappedFrom = submittedVariantEntity.getRemappedFrom();
        this.remappedDate = submittedVariantEntity.getRemappedDate();
        this.remappingId = submittedVariantEntity.getRemappingId();
        this.backPropagatedVariantAccession = submittedVariantEntity.getBackPropagatedVariantAccession();
    }

    @Override
    public String getReferenceSequenceAccession() {
        return referenceSequenceAccession;
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
    public Integer getMapWeight() {
        return mapWeight;
    }

    @Override
    public String getRemappedFrom() {
        return remappedFrom;
    }

    @Override
    public LocalDateTime getRemappedDate() {
        return remappedDate;
    }

    @Override
    public String getRemappingId() {
        return remappingId;
    }

    @Override
    public Long getBackPropagatedVariantAccession() {
        return backPropagatedVariantAccession;
    }

    @Override
    public ISubmittedVariant getModel() {
        return new SubmittedVariant(this);
    }

    public SubmittedVariantEntity toSubmittedVariantEntity() {
        return new SubmittedVariantEntity(this.getAccession(), this.getHashedMessage(), this.getModel(),
                                          this.getVersion(), this.remappedFrom, this.remappedDate, this.remappingId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubmittedVariantInactiveEntity)) {
            return false;
        }

        SubmittedVariantInactiveEntity that = (SubmittedVariantInactiveEntity) o;

        if (taxonomyAccession != that.taxonomyAccession) {
            return false;
        }
        if (start != that.start) {
            return false;
        }
        if (!referenceSequenceAccession.equals(that.referenceSequenceAccession)) {
            return false;
        }
        if (!projectAccession.equals(that.projectAccession)) {
            return false;
        }
        if (!contig.equals(that.contig)) {
            return false;
        }
        if (!referenceAllele.equals(that.referenceAllele)) {
            return false;
        }
        if (!alternateAllele.equals(that.alternateAllele)) {
            return false;
        }
        if (clusteredVariantAccession != null ? !clusteredVariantAccession.equals(
                that.clusteredVariantAccession) : that.clusteredVariantAccession != null) {
            return false;
        }
        if (supportedByEvidence != null ? !supportedByEvidence.equals(
                that.supportedByEvidence) : that.supportedByEvidence != null) {
            return false;
        }
        if (assemblyMatch != null ? !assemblyMatch.equals(that.assemblyMatch) : that.assemblyMatch != null) {
            return false;
        }
        if (allelesMatch != null ? !allelesMatch.equals(that.allelesMatch) : that.allelesMatch != null) {
            return false;
        }
        return validated != null ? validated.equals(that.validated) : that.validated == null;
    }

    @Override
    public int hashCode() {
        int result = referenceSequenceAccession.hashCode();
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

}
