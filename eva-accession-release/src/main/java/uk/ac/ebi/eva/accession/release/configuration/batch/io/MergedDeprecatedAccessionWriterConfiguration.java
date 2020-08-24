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
package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated.MergedDeprecatedVariantAccessionWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_DEPRECATED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_DEPRECATED_RELEASE_WRITER;

@Configuration
public class MergedDeprecatedAccessionWriterConfiguration {

    @Bean(DBSNP_MERGED_DEPRECATED_RELEASE_WRITER)
    public ItemStreamWriter<? super DbsnpClusteredVariantOperationEntity> mergedDeprecatedVariantWriterDbsnp(
            InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpMergedDeprecatedIdsReportPath(parameters.getOutputFolder(),
                                                                                   parameters.getAssemblyAccession());
        return new MergedDeprecatedVariantAccessionWriter(reportPath);
    }

    @Bean(EVA_MERGED_DEPRECATED_RELEASE_WRITER)
    public ItemStreamWriter<? super ClusteredVariantOperationEntity> mergedDeprecatedVariantWriterEva(
            InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaMergedDeprecatedIdsReportPath(parameters.getOutputFolder(),
                                                                                 parameters.getAssemblyAccession());
        return new MergedDeprecatedVariantAccessionWriter(reportPath);
    }

}
