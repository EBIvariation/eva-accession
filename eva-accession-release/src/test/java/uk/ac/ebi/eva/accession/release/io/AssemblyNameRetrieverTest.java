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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class AssemblyNameRetrieverTest {

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Test
    public void xmlParsing() throws IOException, JAXBException {
        File xml = temporaryFolderRule.newFile();
        FileWriter fileWriter = new FileWriter(xml);
        fileWriter.write("<ASSEMBLY accession=\"GCA_000001405.28\" "
                         + "alias=\"GRCh38.p13\" "
                         + "center_name=\"Genome Reference Consortium\">");
        fileWriter.close();
//        XmlMapper xmlMapper = new XmlMapper();
//        xmlMapper.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);
//        EnaAssemblyXml enaAssembly = xmlMapper.readValue(xml, EnaAssemblyXml.class);
        JAXBContext jaxbContext = JAXBContext.newInstance(EnaAssemblyXml.class);

        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

        EnaAssemblyXml enaAssembly = (EnaAssemblyXml) unmarshaller.unmarshal(xml);
        assertEquals("GRCh38.p13", enaAssembly.getAlias());
    }
    @Test
    public void basicRetrieval() throws IOException {
        assertEquals("GRCh38.p13", new AssemblyNameRetriever("GCA_000001405.28").getAssemblyName());
    }
}