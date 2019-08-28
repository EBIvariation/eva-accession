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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Use a curated list and the ENA webservices to return the assembly name associated with an assembly accession.
 *
 * The ENA webservices can return an xml that starts like this:
 * <pre>
 * {@code
 * <?xml version="1.0" encoding="UTF-8"?>
 * <ROOT request="GCA_000001405.28&amp;display=xml">
 * <ASSEMBLY accession="GCA_000001405.28" alias="GRCh38.p13" center_name="Genome Reference Consortium">
 * ...
 * }
 * </pre>
 *
 * However, the alias is optional, according to
 * https://github.com/enasequence/schema/blob/master/src/main/resources/uk/ac/ebi/ena/sra/schema/ENA.assembly.xsd#L32
 * which defines an AssemblyType as an extension of ObjectType, and
 * https://github.com/enasequence/schema/blob/master/src/main/resources/uk/ac/ebi/ena/sra/schema/SRA.common.xsd#L21
 * defines alias as optional attribute of ObjectType.
 */
public class AssemblyNameRetriever {

    private static final String ENA_ASSEMBLY_URL_FORMAT_STRING = "https://www.ebi.ac.uk/ena/data/view/%s";

    private static final String ENA_ASSEMBLY_XML_DISPLAY_SUFFIX = "&display=xml";

    /**
     * These curated assembly names take priority over ENA assembly names because they are used in specific community
     * databases, and these are more likely to be known by users than ENA names.
     */
    private static final Map<String, String> priorityAssemblyNames = ((Supplier<Map<String, String>>) () -> {
        Map<String, String> priorityNames = new HashMap<>();
        priorityNames.put("GCA_000003745.2", "Genoscope.12X");
        return priorityNames;
    }).get();

    private String assemblyAccession;

    private String assemblyName;

    public AssemblyNameRetriever(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
        this.assemblyName = fetchAssemblyName(assemblyAccession);
    }

    private String fetchAssemblyName(String assemblyAccession) {
        if (priorityAssemblyNames.containsKey(assemblyAccession)) {
            return priorityAssemblyNames.get(assemblyAccession);
        } else {
            String url = buildAssemblyUrl(assemblyAccession) + ENA_ASSEMBLY_XML_DISPLAY_SUFFIX;

            try {
                JAXBContext jaxbContext = JAXBContext.newInstance(EnaAssemblyXml.class);
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                EnaAssemblyXml enaAssembly = (EnaAssemblyXml) unmarshaller.unmarshal(new URL(url));
                if (enaAssembly.getAssembly() == null) {
                    return null;
                } else {
                    return enaAssembly.getAssembly().getName();
                }
            } catch (JAXBException | MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String buildAssemblyUrl(String assemblyAccession) {
        return String.format(ENA_ASSEMBLY_URL_FORMAT_STRING, assemblyAccession);
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public Optional<String> getAssemblyName() {
        if (assemblyName == null) {
            return Optional.empty();
        } else {
            return Optional.of(assemblyName);
        }
    }

    public String buildAssemblyUrl() {
        return buildAssemblyUrl(assemblyAccession);
    }

    @XmlRootElement(name = "ROOT")
    static class EnaAssemblyXml {

        @XmlElement(name = "ASSEMBLY")
        private Assembly assembly;

        public EnaAssemblyXml() {
        }

        public Assembly getAssembly() {
            return assembly;
        }

        static class Assembly {

            @XmlElement(name = "NAME")
            private String name;

            public Assembly() {
            }

            String getName() {
                return name;
            }
        }
    }
}
