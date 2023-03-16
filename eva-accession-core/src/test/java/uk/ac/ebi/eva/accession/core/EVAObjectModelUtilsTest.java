/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
 */
package uk.ac.ebi.eva.accession.core;

import org.junit.Test;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.core.EVAObjectModelUtils.*;

public class EVAObjectModelUtilsTest {

    /**
     * {
     "_id" : "F91D9B43DCD5F6F31FC1C0655DCF942EC5BF2F27",
     "seq" : "GCA_000002285.2",
     "tax" : 9615,
     "study" : "BROAD_DBSNP.2005.2.4.16.57",
     "contig" : "CM000028.3",
     "start" : NumberLong(41173146),
     "ref" : "C",
     "alt" : "G",
     "validated" : true,
     "accession" : NumberLong("9187900323"),
     "version" : 1,
     "createdDate" : ISODate("2022-05-26T06:36:31.424Z"),
     "rs" : NumberLong(853087266)
     }
     */
    private static final SubmittedVariant realWorldSV = new SubmittedVariant("GCA_000002285.2", 9615,
            "BROAD_DBSNP.2005.2.4.16.57", "CM000028.3", 41173146, "C", "G", 853087266L, true, true, true, true, null);
    private static final SubmittedVariantEntity realWorldSVE =
            new SubmittedVariantEntity(9187900323L, "F91D9B43DCD5F6F31FC1C0655DCF942EC5BF2F27", realWorldSV, 1);
    /* {
	"_id" : "8A2D7C34562DAC4B6FB824E2CD28D98D0FDAECB2",
	"asm" : "GCA_000002285.2",
	"tax" : 9615,
	"contig" : "CM000028.3",
	"start" : NumberLong(41173146),
	"type" : "SNV",
	"accession" : NumberLong(853087266),
	"version" : 1,
	"createdDate" : ISODate("2015-11-19T08:59:00Z")
    }*/
    private static final ClusteredVariant realWorldCV = new ClusteredVariant("GCA_000002285.2", 9615,
            "CM000028.3", 41173146, VariantType.SNV, true, null);
    private static final ClusteredVariantEntity realWorldCVE =
            new ClusteredVariantEntity(853087266L, "8A2D7C34562DAC4B6FB824E2CD28D98D0FDAECB2", realWorldCV, 1);

    @Test
    public void testGetClusteredVariantHash() {
        assertEquals(realWorldCVE.getHashedMessage(), getClusteredVariantHash(realWorldSV));
    }

    @Test
    public void testToClusteredVariant() {
        assertEquals(realWorldCV, toClusteredVariant(realWorldSV));
        SubmittedVariant realWorldSVWithMapWeight = new SubmittedVariant(realWorldSV.getReferenceSequenceAccession(),
                realWorldSV.getTaxonomyAccession(), realWorldSV.getProjectAccession(), realWorldSV.getContig(),
                realWorldSV.getStart(),
                realWorldSV.getReferenceAllele(), realWorldSV.getAlternateAllele(),
                realWorldSV.getClusteredVariantAccession());
        realWorldSVWithMapWeight.setMapWeight(3);
        assertEquals(3, toClusteredVariant(realWorldSVWithMapWeight).getMapWeight().intValue());
    }

    @Test
    public void testSubmittedVariantEntityToClusteredVariantEntity() {
        assertEquals(realWorldCVE, toClusteredVariantEntity(realWorldSVE));
    }

    @Test
    public void testClusteredVariantToClusteredVariantEntity() {
        assertEquals(realWorldCVE, toClusteredVariantEntity(realWorldCVE.getAccession(), realWorldCV));
    }

    @Test
    public void testSubmittedVariantToSubmittedVariantEntity() {
        assertEquals(realWorldSVE, toSubmittedVariantEntity(realWorldSVE.getAccession(), realWorldSV));
    }
}
