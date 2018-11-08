/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.test.configuration;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import java.io.IOException;

//@Configuration
public class InputParametersTestConfiguration {

//    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();
//
//    @Bean
//    @ConfigurationProperties(prefix = "parameters")
//    public InputParameters inputParameters() throws IOException {
//        InputParameters inputParameters = new InputParameters();
//        inputParameters.setOutputVcf(temporaryFolderRule.newFile().getAbsolutePath());
//        return inputParameters;
//    }
}
