/*
 *
 * Copyright 2020 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;

@Document
public class ClusteredVariantEntity extends AccessionedDocument<IClusteredVariant, Long> implements IClusteredVariant {

    @Indexed(background = true)
    @Field("asm")
    private String assemblyAccession;

    @Field("tax")
    private int taxonomyAccession;

    private String contig;

    private long start;

    private VariantType type;

    private Boolean validated;

    protected ClusteredVariantEntity() {
    }

    public ClusteredVariantEntity(Long accession, String hashedMessage, IClusteredVariant model) {
        this(accession, hashedMessage, model, 1);
    }

    public ClusteredVariantEntity(Long accession, String hashedMessage, IClusteredVariant model, int version) {
        this(accession, hashedMessage, model.getAssemblyAccession(), model.getTaxonomyAccession(), model.getContig(),
             model.getStart(), model.getType(), model.isValidated(), model.getCreatedDate(), version);
    }

    public ClusteredVariantEntity(Long accession, String hashedMessage, String assemblyAccession,
                                       int taxonomyAccession, String contig, long start, VariantType type,
                                       Boolean validated, LocalDateTime createdDate, int version) {
        super(hashedMessage, accession, version);
        this.setCreatedDate(createdDate);
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.contig = contig;
        this.start = start;
        this.type = type;
        if (validated == null) {
            throw new IllegalArgumentException("validated should not be null, as null is used for default values");
        } else {
            this.validated = validated == DEFAULT_VALIDATED ? null : validated;
        }
    }

    public IClusteredVariant getModel() {
        ClusteredVariant clusteredVariant = new ClusteredVariant(this);
        clusteredVariant.setValidated(isValidated());
        return clusteredVariant;
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
    public String getContig() {
        return contig;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public VariantType getType() {
        return type;
    }

    @Override
    public Boolean isValidated() {
        return validated == null ? DEFAULT_VALIDATED : validated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusteredVariantEntity)) {
            return false;
        }

        ClusteredVariantEntity that = (ClusteredVariantEntity) o;

        if (taxonomyAccession != that.taxonomyAccession) {
            return false;
        }
        if (start != that.start) {
            return false;
        }
        if (!assemblyAccession.equals(that.assemblyAccession)) {
            return false;
        }
        if (!contig.equals(that.contig)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        return validated != null ? validated.equals(that.validated) : that.validated == null;
    }

    @Override
    public int hashCode() {
        int result = assemblyAccession.hashCode();
        result = 31 * result + taxonomyAccession;
        result = 31 * result + contig.hashCode();
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + type.hashCode();
        result = 31 * result + (validated != null ? validated.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusteredVariantEntity{"
                + "assemblyAccession='" + assemblyAccession + '\''
                + ", taxonomyAccession=" + taxonomyAccession
                + ", contig='" + contig + '\''
                + ", start=" + start
                + ", type=" + type
                + ", validated=" + validated
                + '}';
    }

}
