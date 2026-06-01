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
package uk.ac.ebi.eva.accession.release.assembly;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.eva.accession.core.utils.PipelineTemporaryFolderUtil;
import uk.ac.ebi.eva.accession.release.assembly.AssemblyNameRetriever.EnaAssemblyXml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssemblyNameRetrieverTest {
    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @Test
    public void parseXml() throws IOException, JAXBException {
        File xml = temporaryFolderUtil.newFile();
        FileWriter fileWriter = new FileWriter(xml);
        fileWriter.write("<ASSEMBLY_SET><ASSEMBLY accession=\"GCA_000001405.28\">\n" +
                "<NAME>GRCh38.p13</NAME>\n" +
                "</ASSEMBLY></ASSEMBLY_SET>");
        fileWriter.close();

        JAXBContext jaxbContext = JAXBContext.newInstance(EnaAssemblyXml.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        EnaAssemblyXml enaAssembly = (EnaAssemblyXml) unmarshaller.unmarshal(xml);
        assertEquals("GRCh38.p13", enaAssembly.getAssembly().getName());
    }

    @Test
    public void parseMissingName() throws IOException, JAXBException {
        File xml = temporaryFolderUtil.newFile();
        FileWriter fileWriter = new FileWriter(xml);
        fileWriter.write("<ASSEMBLY_SET><ASSEMBLY></ASSEMBLY></ASSEMBLY_SET>");
        fileWriter.close();

        JAXBContext jaxbContext = JAXBContext.newInstance(EnaAssemblyXml.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        EnaAssemblyXml enaAssembly = (EnaAssemblyXml) unmarshaller.unmarshal(xml);
        assertNull(enaAssembly.getAssembly().getName());
    }

    @Test
    public void retrieve() {
        assertEquals("GRCh38.p13", new AssemblyNameRetriever("GCA_000001405.28").getAssemblyName().get());
    }

    @Test
    public void retrieveAssemblyWithWrongFormat() {
        assertThrows(RuntimeException.class, () -> new AssemblyNameRetriever("GCA_wrong_format"));
    }

    @Test
    public void retrieveNonExistentAssembly() {
        assertFalse(new AssemblyNameRetriever("GCA_000000000.1").getAssemblyName().isPresent());
    }

    @Test
    public void buildHumanReadableUrl() {
        assertEquals("https://www.ebi.ac.uk/ena/browser/view/GCA_000001405.28",
                new AssemblyNameRetriever("GCA_000001405.28").buildAssemblyHumanReadableUrl());
    }

    @Test
    public void retrieveNameWithMorePriorityThanEnaName() {
        assertEquals("Genoscope.12X", new AssemblyNameRetriever("GCA_000003745.2").getAssemblyName().get());
    }
}
