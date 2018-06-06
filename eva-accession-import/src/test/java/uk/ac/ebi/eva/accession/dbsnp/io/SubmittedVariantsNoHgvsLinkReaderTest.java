/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.dbsnp.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.dbsnp.configuration.TestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.model.VariantNoHgvsLink;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {TestConfiguration.class})
public class SubmittedVariantsNoHgvsLinkReaderTest {

    private static final int PAGE_SIZE = 10;

    private SubmittedVariantsNoHgvsLinkReader reader;

    private static final String CHICKEN_ASSEMBY = "Gallus_gallus-5.0";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Autowired
    private DbsnpDataSource dbsnpDataSource;

    @After
    public void tearDown() {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    public void readChickenVariants() throws Exception {
        reader = buildReader(11825, CHICKEN_ASSEMBY, PAGE_SIZE);
        List<VariantNoHgvsLink> variants = readAll(reader);
        assertEquals(2, variants.size());
    }

    @Test
    public void readBatchWithNoVariants() throws Exception {
        reader = buildReader(11000, CHICKEN_ASSEMBY, PAGE_SIZE);
        assertNull(reader.read());
    }

    @Test
    public void readWrongAssembly() throws Exception {
        thrown.expect(ItemStreamException.class);
        reader = buildReader(11825, "UNKNOWN_ASSEMBLY", PAGE_SIZE);
    }

    private SubmittedVariantsNoHgvsLinkReader buildReader(int batch, String assembly, int pageSize)
            throws Exception {
        SubmittedVariantsNoHgvsLinkReader fieldsReader = new SubmittedVariantsNoHgvsLinkReader(batch, assembly, dbsnpDataSource.getDatasource(), pageSize);
        fieldsReader.afterPropertiesSet();
        ExecutionContext executionContext = new ExecutionContext();
        fieldsReader.open(executionContext);
        return fieldsReader;
    }

    private List<VariantNoHgvsLink> readAll(SubmittedVariantsNoHgvsLinkReader reader) throws Exception {
        List<VariantNoHgvsLink> variants = new ArrayList<>();

        VariantNoHgvsLink variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }

        return variants;
    }
}