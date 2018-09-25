/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor.DECLUSTERED;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor.DECLUSTERED_ALLELES_MISMATCH;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor.DECLUSTERED_TYPE_MISMATCH;

public class SubmittedVariantDeclusterProcessorTest {

    private static final String ASSEMBLY = "AnyAssembly-1.0";

    private static final String PROJECT_ACCESSION = "PROJECT";

    private static final String CONTIG = "CONTIG";

    private static final int TAXONOMY = 3880;

    private static final int START = 100;

    private static final Long CLUSTERED_VARIANT_ACCESSION = 12L;

    private static final Long SUBMITTED_VARIANT_ACCESSION = 15L;

    private static SubmittedVariantDeclusterProcessor processor;

    private static Function<ISubmittedVariant, String> hashingFunctionSubmitted;

    private static Function<IClusteredVariant, String> hashingFunctionClustered;

    @BeforeClass
    public static void setUpClass() throws Exception {
        processor = new SubmittedVariantDeclusterProcessor();
        hashingFunctionSubmitted = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        hashingFunctionClustered = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Test
    public void processSubmittedVariantAllelesMismatch() throws Exception {
        List<DbsnpSubmittedVariantEntity> submittedVariantEntities = createSubmittedVariantEntities
                ("A", new String[] {"C"}, false);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(submittedVariantEntities, DbsnpVariantType.SNV,
                                                          VariantType.SNV);
        processor.process(wrapper);

        //Check Decluster
        assertEquals(1, wrapper.getSubmittedVariants().size());
        assertNull(wrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());

        //Check Operations
        assertOperation(wrapper, getReason(Collections.singletonList(DECLUSTERED_ALLELES_MISMATCH)),
                        wrapper.getSubmittedVariants().get(0).getAccession(), 0);
    }

    @Test
    public void processSubmittedVariantTypeMismatch() throws Exception {
        List<DbsnpSubmittedVariantEntity> submittedVariantEntities = createSubmittedVariantEntities
                ("G", new String[] {"-", "G", "T", "TT"}, true);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(submittedVariantEntities, DbsnpVariantType.DIV,
                                                          VariantType.DEL);
        processor.process(wrapper);

        //Check Decluster
        assertEquals(3, wrapper.getSubmittedVariants().size());
        assertNotNull(wrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());
        assertNull(wrapper.getSubmittedVariants().get(1).getClusteredVariantAccession());
        assertNull(wrapper.getSubmittedVariants().get(2).getClusteredVariantAccession());

        //Check Operations
        assertEquals(2, wrapper.getOperations().size());
        assertOperation(wrapper, getReason(Collections.singletonList(DECLUSTERED_TYPE_MISMATCH)),
                         wrapper.getSubmittedVariants().get(0).getAccession(), 0);
        assertOperation(wrapper, getReason(Collections.singletonList(DECLUSTERED_TYPE_MISMATCH)),
                         wrapper.getSubmittedVariants().get(1).getAccession(), 1);
    }

    @Test
    public void processSubmittedVariantAllelesAndTypeMismatch() throws Exception {
        List<DbsnpSubmittedVariantEntity> submittedVariantEntities = createSubmittedVariantEntities
                ("G", new String[] {"-", "T", "TT"}, false);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(submittedVariantEntities, DbsnpVariantType.DIV,
                                                          VariantType.DEL);
        processor.process(wrapper);

        //Check Decluster
        assertEquals(3, wrapper.getSubmittedVariants().size());
        assertNull(wrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());
        assertNull(wrapper.getSubmittedVariants().get(1).getClusteredVariantAccession());
        assertNull(wrapper.getSubmittedVariants().get(2).getClusteredVariantAccession());

        //Check Operations
        assertEquals(3, wrapper.getOperations().size());
        assertOperation(wrapper, getReason(Collections.singletonList(DECLUSTERED_ALLELES_MISMATCH)),
                        wrapper.getSubmittedVariants().get(0).getAccession(), 0);
        assertOperation(wrapper, getReason(Arrays.asList(DECLUSTERED_ALLELES_MISMATCH, DECLUSTERED_TYPE_MISMATCH)),
                        wrapper.getSubmittedVariants().get(1).getAccession(), 1);
        assertOperation(wrapper, getReason(Arrays.asList(DECLUSTERED_ALLELES_MISMATCH, DECLUSTERED_TYPE_MISMATCH)),
                        wrapper.getSubmittedVariants().get(2).getAccession(), 2);
    }

    private List<DbsnpSubmittedVariantEntity> createSubmittedVariantEntities(String reference, String[] alternates,
                                                                             boolean allelesMatch) {
        List<DbsnpSubmittedVariantEntity> submittedVariantEntities = new ArrayList<>();
        for (String alternate : alternates) {
            if (alternate.equals("-")) {
                alternate = "";
            }
            if (!reference.equals(alternate)) {
                SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT_ACCESSION, CONTIG,
                                                                         START, reference, alternate,
                                                                         CLUSTERED_VARIANT_ACCESSION,
                                                                         DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                         DEFAULT_ASSEMBLY_MATCH, allelesMatch,
                                                                         DEFAULT_VALIDATED, null);
                DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                        SUBMITTED_VARIANT_ACCESSION, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant,
                        1);
                submittedVariantEntities.add(submittedVariantEntity);
            }
        }
        return submittedVariantEntities;
    }

    private DbsnpVariantsWrapper buildSimpleWrapper(List<DbsnpSubmittedVariantEntity> submittedVariantEntities,
                                                    DbsnpVariantType dbsnpVariantType,
                                                    VariantType clusteredVariantType) {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY, "contig", START,
                                                                 clusteredVariantType, DEFAULT_VALIDATED, null);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                SUBMITTED_VARIANT_ACCESSION, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setSubmittedVariants(submittedVariantEntities);
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setDbsnpVariantType(dbsnpVariantType);
        return wrapper;
    }

    private void assertOperation(DbsnpVariantsWrapper dbsnpVariantsWrapper, String reason, Long accession, int index) {
        assertEquals(EventType.UPDATED, dbsnpVariantsWrapper.getOperations().get(index).getEventType());
        assertEquals(1, dbsnpVariantsWrapper.getOperations().get(index).getInactiveObjects().size());
        assertEquals(reason, dbsnpVariantsWrapper.getOperations().get(index).getReason());
        assertEquals(accession, dbsnpVariantsWrapper.getOperations().get(index).getAccession());
    }

    private String getReason(List<String> reasons){
        return DECLUSTERED + String.join(" ", reasons);
    }

}
