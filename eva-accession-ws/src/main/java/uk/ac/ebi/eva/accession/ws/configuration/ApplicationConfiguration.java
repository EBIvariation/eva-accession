/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.ws.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.web.HttpMessageConverters;
import org.springframework.boot.autoconfigure.web.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebClientAutoConfiguration;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableBasicRestControllerAdvice;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.ws.response.NonRedirectingClientHttpRequestFactory;

import java.util.List;

@Configuration
@EnableBasicRestControllerAdvice
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@AutoConfigureAfter(HttpMessageConvertersAutoConfiguration.class)
public class ApplicationConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }

    @Bean
    public BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicClusteredRestController(
            ClusteredVariantAccessioningService service) {
        return new BasicRestController<>(service, ClusteredVariant::new);
    }

    @Bean
    public BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicSubmittedRestController(
            SubmittedVariantAccessioningService service) {
        return new BasicRestController<>(service, SubmittedVariant::new);
    }

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurerAdapter() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**");
            }
        };
    }

    /**
     * Implementation reused from {@link WebClientAutoConfiguration}, but with an extra call to the method
     * {@link org.springframework.boot.web.client.RestTemplateBuilder#requestFactory} to provide our
     * {@link NonRedirectingClientHttpRequestFactory}.
     */
    @Bean
    public RestTemplateBuilder restTemplateBuilder(
            ObjectProvider<HttpMessageConverters> messageConverters,
            ObjectProvider<List<RestTemplateCustomizer>> restTemplateCustomizers) {
        WebClientAutoConfiguration.RestTemplateConfiguration restTemplateConfiguration =
                new WebClientAutoConfiguration.RestTemplateConfiguration(
                        messageConverters, restTemplateCustomizers);
        RestTemplateBuilder builder = restTemplateConfiguration.restTemplateBuilder();

        builder = builder.requestFactory(new NonRedirectingClientHttpRequestFactory());

        return builder;
    }
}
