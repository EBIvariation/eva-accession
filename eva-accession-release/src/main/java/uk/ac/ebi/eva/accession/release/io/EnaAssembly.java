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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "ROOT")
class EnaAssemblyXml {

    @XmlElement(name = "ASSEMBLY")
    private Assembly assembly;

    public EnaAssemblyXml() {
    }

    public void setAssembly(Assembly assembly) {
        this.assembly = assembly;
    }

    public String getAlias() {
        return assembly.getAlias();
    }

    static class Assembly {

        @XmlAttribute
        private String alias;

        public Assembly() {
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        String getAlias() {
            return alias;
        }
    }
}
