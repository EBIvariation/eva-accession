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
package uk.ac.ebi.eva.accession.release.batch.io.deprecated;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

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
        File output = temporaryFolderRule.newFile();
        deprecatedVariantAccessionWriter = new DeprecatedVariantAccessionWriter(output.toPath());
    }

    @Test
    public void write() throws Exception {
        Variant variant1 = new Variant("chr1", 1L, 2L, "C", "A");
        variant1.setMainId("rs1");
        Variant variant2 = new Variant("chr2", 2L, 3L, "A", "T");
        variant2.setMainId("rs2");
        Variant variant3 = new Variant("chr3", 3L, 4L, "T", "G");
        variant3.setMainId("rs3");

        deprecatedVariantAccessionWriter.open(null);
        deprecatedVariantAccessionWriter.write(Arrays.asList(variant1, variant2, variant3));
        deprecatedVariantAccessionWriter.close();

        assertEquals(3, numberOfLines(deprecatedVariantAccessionWriter.getOutput()));
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }

}