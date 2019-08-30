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

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.AccessionReleaseJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.IllegalStartSkipPolicyConfiguration;

@EnableAutoConfiguration
@Import({MongoConfiguration.class,
         InputParametersConfiguration.class,
         AccessionReleaseJobConfiguration.class,
         ChunkSizeCompletionPolicyConfiguration.class,
         IllegalStartSkipPolicyConfiguration.class
})
public class BatchTestConfiguration {

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }
}
