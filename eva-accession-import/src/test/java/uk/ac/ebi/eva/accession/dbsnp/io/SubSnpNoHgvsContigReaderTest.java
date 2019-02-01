/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.io.SubSnpNoHgvsContigReader;
import uk.ac.ebi.eva.accession.core.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.dbsnp.test.TestConfiguration;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {TestConfiguration.class})
public class SubSnpNoHgvsContigReaderTest {

    private static final String CHICKEN_ASSEMBLY_5 = "Gallus_gallus-5.0";

    private static final String CHICKEN_ASSEMBLY_4 = "Gallus_gallus-4.0";

    private static final long BUILD_NUMBER = 145L;

    private static final int PAGE_SIZE = 10;

    private SubSnpNoHgvsContigReader reader;

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
        reader = buildReader(CHICKEN_ASSEMBLY_5, PAGE_SIZE);
        List<String> variants = readAll(reader);
        assertEquals(6, variants.size());
    }

    @Test
    public void readPreviousBuildChickenVariants() throws Exception {
        reader = buildReader(CHICKEN_ASSEMBLY_4, BUILD_NUMBER, PAGE_SIZE);
        List<String> variants = readAll(reader);
        assertEquals(3, variants.size());
    }

    private SubSnpNoHgvsContigReader buildReader(String assembly, int pageSize) throws Exception {
        return buildReader(assembly, null, pageSize);
    }

    private SubSnpNoHgvsContigReader buildReader(String assembly, Long buildNumber, int pageSize) throws Exception {
        SubSnpNoHgvsContigReader fieldsReader = new SubSnpNoHgvsContigReader(assembly, buildNumber, dbsnpDataSource.getDatasource(), pageSize);
        fieldsReader.afterPropertiesSet();
        ExecutionContext executionContext = new ExecutionContext();
        fieldsReader.open(executionContext);
        return fieldsReader;
    }

    private List<String> readAll(SubSnpNoHgvsContigReader reader) throws Exception {
        List<String> variants = new ArrayList<>();

        String variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }

        return variants;
    }
}