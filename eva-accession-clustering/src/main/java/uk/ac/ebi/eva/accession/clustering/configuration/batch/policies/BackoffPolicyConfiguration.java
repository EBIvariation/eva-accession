/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.policies;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;

@Configuration
public class BackoffPolicyConfiguration {

    @Bean
    @StepScope
    public BackOffPolicy exponentialRandomBackoffPolicy() {
        // TODO could use InputParameters
        ExponentialRandomBackOffPolicy backOffPolicy = new ExponentialRandomBackOffPolicy();
        backOffPolicy.setInitialInterval(100L);
        backOffPolicy.setMaxInterval(30000L);
        backOffPolicy.setMultiplier(2.0);
        return backOffPolicy;
    }

}
