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
package uk.ac.ebi.eva.accession.dbsnp.batch.io;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.core.test.configuration.TestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.model.CoordinatesPresence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@TestPropertySource({"classpath:contig-test.properties"})
@ContextConfiguration(classes = {TestConfiguration.class})
public class SubSnpNoHgvsContigAndChromosomeReaderTest {

    private static final String CHICKEN_ASSEMBLY_5 = "Gallus_gallus-5.0";

    private static final String CHICKEN_ASSEMBLY_4 = "Gallus_gallus-4.0";

    private static final String BUILD_NUMBER = "145";

    private static final int PAGE_SIZE = 10;

    private SubSnpNoHgvsContigAndChromosomeReader reader;

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
        List<CoordinatesPresence> coordinates = readAll(reader);
        assertEquals(7, coordinates.size());
        assertEquals(new HashSet<>(Arrays.asList(new CoordinatesPresence("22", true, "NT_455866"),
                                                 new CoordinatesPresence("22", true, "NT_455837"),
                                                 new CoordinatesPresence(null, true, "NT_464165.1"),
                                                 new CoordinatesPresence(null, true, "AADN04000814.1"),
                                                 new CoordinatesPresence(null, true, "chrUn_Scaffold827"),
                                                 new CoordinatesPresence(null, false, "AADN04000814.1"),
                                                 new CoordinatesPresence("22", false, "CTG1"))),
                     new HashSet<>(coordinates));
    }

    @Test
    public void readPreviousBuildChickenVariants() throws Exception {
        reader = buildReader(CHICKEN_ASSEMBLY_4, BUILD_NUMBER, PAGE_SIZE);
        List<CoordinatesPresence> coordinates = readAll(reader);
        assertEquals(3, coordinates.size());
        assertEquals(new HashSet<>(Arrays.asList(new CoordinatesPresence("1", true, "NW_003763476.1"),
                                                 new CoordinatesPresence("12", true, "NW_003763901.1"),
                                                 new CoordinatesPresence("3", true, "NW_003763686.1"))),
                     new HashSet<>(coordinates));
    }

    private SubSnpNoHgvsContigAndChromosomeReader buildReader(String assembly, int pageSize) throws Exception {
        return buildReader(assembly, null, pageSize);
    }

    private SubSnpNoHgvsContigAndChromosomeReader buildReader(String assembly, String buildNumber, int pageSize) throws Exception {
        SubSnpNoHgvsContigAndChromosomeReader fieldsReader = new SubSnpNoHgvsContigAndChromosomeReader(assembly, buildNumber, dbsnpDataSource.getDatasource(), pageSize);
        fieldsReader.afterPropertiesSet();
        ExecutionContext executionContext = new ExecutionContext();
        fieldsReader.open(executionContext);
        return fieldsReader;
    }

    private List<CoordinatesPresence> readAll(SubSnpNoHgvsContigAndChromosomeReader reader) throws Exception {
        List<CoordinatesPresence> coordinatesPresences = new ArrayList<>();

        CoordinatesPresence coordinatesPresence;
        while ((coordinatesPresence = reader.read()) != null) {
            coordinatesPresences.add(coordinatesPresence);
        }

        return coordinatesPresences;
    }
}