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
import org.springframework.data.mongodb.core.mapping.Field;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;

public class ClusteredVariantInactiveEntity extends InactiveSubDocument<IClusteredVariant, Long> implements IClusteredVariant {

    @Indexed(background = true)
    @Field("asm")
    private String assemblyAccession;

    @Field("tax")
    private int taxonomyAccession;

    private String contig;

    private long start;

    private VariantType type;

    private Boolean validated;

    private Integer mapWeight;

    public ClusteredVariantInactiveEntity() {
        super();
    }

    public ClusteredVariantInactiveEntity(ClusteredVariantEntity clusteredVariantEntity) {
        super(clusteredVariantEntity);
        this.assemblyAccession = clusteredVariantEntity.getAssemblyAccession();
        this.taxonomyAccession = clusteredVariantEntity.getTaxonomyAccession();
        this.contig = clusteredVariantEntity.getContig();
        this.start = clusteredVariantEntity.getStart();
        this.type = clusteredVariantEntity.getType();
        this.validated = clusteredVariantEntity.isValidated();
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
        return validated;
    }

    @Override
    public Integer getMapWeight() {
        return mapWeight;
    }

    @Override
    public IClusteredVariant getModel() {
        return new ClusteredVariant(this);
    }

    public ClusteredVariantEntity toClusteredVariantEntity() {
        return new ClusteredVariantEntity(this.getAccession(), this.getHashedMessage(), this.getModel(),
                                          this.getVersion());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusteredVariantInactiveEntity that = (ClusteredVariantInactiveEntity) o;

        if (taxonomyAccession != that.taxonomyAccession) return false;
        if (start != that.start) return false;
        if (!assemblyAccession.equals(that.assemblyAccession)) return false;
        if (!contig.equals(that.contig)) return false;
        if (type != that.type) return false;
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
        return "ClusteredVariantInactiveEntity{"
                + "hashedMessage='" + getHashedMessage() + '\''
                + ", assemblyAccession='" + assemblyAccession + '\''
                + ", taxonomyAccession=" + taxonomyAccession
                + ", contig='" + contig + '\''
                + ", start=" + start
                + ", type=" + type
                + ", validated=" + validated
                + '}';
    }
}
