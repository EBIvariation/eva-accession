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
package uk.ac.ebi.eva.accession.dbsnp2.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.dbsnp2.io.BzipLazyResource;
import uk.ac.ebi.eva.accession.dbsnp2.io.JsonNodeLineMapper;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import java.io.File;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;

/**
 * Configuration to inject a NDJson reader as a Variant Reader bean.
 */
@Configuration
@EnableConfigurationProperties({DbsnpDataSource.class})
public class ImportDbsnpJsonVariantsReaderConfiguration {

    @Bean(name = DBSNP_JSON_VARIANT_READER)
    @StepScope
    FlatFileItemReader<JsonNode> dbsnpJsonItemReader(InputParameters parameters) {
        String sourceFileName = parameters.getInput();
        FlatFileItemReader<JsonNode> jsonReader = new FlatFileItemReader<>();
        jsonReader.setName("DbsnpJsonItemReader");
        jsonReader.setResource(new BzipLazyResource(new File(sourceFileName)));
        jsonReader.setLineMapper(new JsonNodeLineMapper());
        return jsonReader;
    }
}
