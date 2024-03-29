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
package uk.ac.ebi.eva.remapping.source.parameters;


import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resolves the paths to the reports generated in a RefSNP ID release.
 */
public class ReportPathResolver {

    public static Path getEvaReportPath(String outputFolder, String referenceAssembly, int taxonomy) {
        final String FILE_SUFFIX = "_eva.vcf";
        return Paths.get(outputFolder).resolve(referenceAssembly + '_' + taxonomy + FILE_SUFFIX);
    }

    public static Path getDbsnpReportPath(String outputFolder, String referenceAssembly, int taxonomy) {
        final String FILE_SUFFIX = "_dbsnp.vcf";
        return Paths.get(outputFolder).resolve(referenceAssembly + '_' + taxonomy + FILE_SUFFIX);
    }
}
