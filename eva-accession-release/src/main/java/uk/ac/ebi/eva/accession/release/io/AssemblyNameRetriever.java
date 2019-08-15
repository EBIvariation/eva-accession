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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.springframework.core.io.UrlResource;

import java.io.IOException;
import java.net.URL;

public class AssemblyNameRetriever {

    private static final String ENA_ASSMEBLY_URL_FORMAT_STRING = "https://www.ebi.ac.uk/ena/data/view/%s&display=xml";

    private String assemblyAccession;

    private String assemblyName;

    public AssemblyNameRetriever(String assemblyAccession) throws IOException {
        this.assemblyAccession = assemblyAccession;
        this.assemblyName = fetchAssemblyName(assemblyAccession);
    }

    private String fetchAssemblyName(String assemblyAccession) throws IOException {
        String url = String.format(ENA_ASSMEBLY_URL_FORMAT_STRING, assemblyAccession);
        UrlResource resource = new UrlResource(url);
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);
        EnaAssemblyXml enaAssembly = xmlMapper.readValue(new URL(url), EnaAssemblyXml.class);
        return enaAssembly.getAlias();
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public String getAssemblyName() {
        return assemblyName;
    }
}
