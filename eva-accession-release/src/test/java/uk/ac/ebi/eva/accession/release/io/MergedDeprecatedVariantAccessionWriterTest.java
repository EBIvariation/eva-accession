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

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergedDeprecatedVariantAccessionWriterTest {

    private MergedDeprecatedVariantAccessionWriter writer;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        File output = temporaryFolderRule.newFile();
        writer = new MergedDeprecatedVariantAccessionWriter(output.toPath());
    }

    @Test
    public void writeLines() throws Exception {
        DbsnpClusteredVariantOperationEntity variant1 = new DbsnpClusteredVariantOperationEntity();
        variant1.fill(EventType.MERGED, 1L, 11L, "Reason", null);
        DbsnpClusteredVariantOperationEntity variant2 = new DbsnpClusteredVariantOperationEntity();
        variant2.fill(EventType.MERGED, 2L, 12L, "Reason", null);
        DbsnpClusteredVariantOperationEntity variant3 = new DbsnpClusteredVariantOperationEntity();
        variant3.fill(EventType.MERGED, 3L, 13L, "Reason", null);

        writer.open(null);
        writer.write(Arrays.asList(variant1, variant2, variant3));
        writer.close();

        assertEquals(3, numberOfLines(writer.getOutput()));
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }

    @Test
    public void twoColumns() throws Exception {
        DbsnpClusteredVariantOperationEntity variant1 = new DbsnpClusteredVariantOperationEntity();
        long accessionIdOrigin = 1L;
        long accessionIdDestiny = 11L;
        variant1.fill(EventType.MERGED, accessionIdOrigin, accessionIdDestiny, "Reason", null);
        writer.open(null);
        writer.write(Arrays.asList(variant1));
        writer.close();

        Optional<String> line = new BufferedReader(new FileReader(
                writer.getOutput()))
                .lines().findFirst();
        assertTrue(line.isPresent());
        assertEquals(line.get(), "rs" + accessionIdOrigin + "\trs" + accessionIdDestiny);
    }

}