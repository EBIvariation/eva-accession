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
package uk.ac.ebi.eva.accession.release.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DeprecatedVariantAccessionWriterTest {

    private DeprecatedVariantAccessionWriter deprecatedVariantAccessionWriter;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        File output = temporaryFolderRule.newFolder();
        deprecatedVariantAccessionWriter = new DeprecatedVariantAccessionWriter(output.getAbsolutePath(), "assembly");
    }

    @Test
    public void write() throws Exception {
        DbsnpClusteredVariantOperationEntity variant1 = new DbsnpClusteredVariantOperationEntity();
        variant1.fill(EventType.DEPRECATED, 1L, null, "Reason", null);
        DbsnpClusteredVariantOperationEntity variant2 = new DbsnpClusteredVariantOperationEntity();
        variant2.fill(EventType.DEPRECATED, 2L, null, "Reason", null);
        DbsnpClusteredVariantOperationEntity variant3 = new DbsnpClusteredVariantOperationEntity();
        variant3.fill(EventType.DEPRECATED, 3L, null, "Reason", null);

        deprecatedVariantAccessionWriter.open(null);
        deprecatedVariantAccessionWriter.write(Arrays.asList(variant1, variant2, variant3));
        deprecatedVariantAccessionWriter.close();

        assertEquals(3, numberOfLines(deprecatedVariantAccessionWriter.getOutputPath().toFile()));
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }

}