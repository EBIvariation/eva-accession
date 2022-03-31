/*
 *
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.core.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasIntputParameters;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;

@Configuration
public class ContigAliasConfiguration {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Bean
    @ConfigurationProperties(prefix = "contig-alias")
    public ContigAliasIntputParameters contigAliasIntputParameters() {
        return new ContigAliasIntputParameters();
    }

    @Bean
    public ContigAliasService contigAliasService(RestTemplate restTemplate,
                                                 ContigAliasIntputParameters contigAliasIntputParameters) {
        return new ContigAliasService(restTemplate, contigAliasIntputParameters.getUrl());
    }
}
