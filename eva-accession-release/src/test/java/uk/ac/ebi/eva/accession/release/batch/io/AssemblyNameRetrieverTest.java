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
package uk.ac.ebi.eva.accession.release.batch.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.release.batch.io.AssemblyNameRetriever.EnaAssemblyXml;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class AssemblyNameRetrieverTest {

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Test
    public void parseXml() throws IOException, JAXBException {
        File xml = temporaryFolderRule.newFile();
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
        File xml = temporaryFolderRule.newFile();
        FileWriter fileWriter = new FileWriter(xml);
        fileWriter.write("<ASSEMBLY_SET><ASSEMBLY></ASSEMBLY></ASSEMBLY_SET>");
        fileWriter.close();

        JAXBContext jaxbContext = JAXBContext.newInstance(EnaAssemblyXml.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        EnaAssemblyXml enaAssembly = (EnaAssemblyXml) unmarshaller.unmarshal(xml);
        assertNull(enaAssembly.getAssembly().getName());
    }

    @Test
    public void retrieve() throws IOException, JAXBException {
        assertEquals("GRCh38.p13", new AssemblyNameRetriever("GCA_000001405.28").getAssemblyName().get());
    }

    @Test
    public void retrieveAssemblyWithWrongFormat() throws IOException, JAXBException {
        assertThrows(RuntimeException.class, () -> new AssemblyNameRetriever("GCA_wrong_format"));
    }

    @Test
    public void retrieveNonExistentAssembly() throws IOException, JAXBException {
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
