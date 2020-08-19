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
package uk.ac.ebi.eva.accession.release.parameters;


import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Resolves the paths to the reports generated in a RefSNP ID release.
 */
public class ReportPathResolver {

    public static final String CURRENT_FILE_SUFFIX = "_current_ids.vcf";

    public static final String MERGED_FILE_SUFFIX = "_merged_ids.vcf";

    public static final String DEPRECATED_FILE_SUFFIX = "_deprecated_ids.unsorted.txt";

    public static final String MERGED_DEPRECATED_FILE_SUFFIX = "_merged_deprecated_ids.unsorted.txt";

    public static final String MULTIMAP_FILE_SUFFIX = "_multimap_ids.vcf";

    public static final String DBSNP_PREFIX = "dbsnp_";


    public static Path getDbsnpCurrentIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(DBSNP_PREFIX + referenceAssembly + CURRENT_FILE_SUFFIX);
    }

    public static Path getDbsnpMergedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(DBSNP_PREFIX + referenceAssembly + MERGED_FILE_SUFFIX);
    }

    public static Path getDbsnpDeprecatedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(DBSNP_PREFIX + referenceAssembly + DEPRECATED_FILE_SUFFIX);
    }

    public static Path getDbsnpMergedDeprecatedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(DBSNP_PREFIX + referenceAssembly + MERGED_DEPRECATED_FILE_SUFFIX);
    }

    public static Path getDbsnpMultimapIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(DBSNP_PREFIX + referenceAssembly + MULTIMAP_FILE_SUFFIX);
    }

    public static Path getEvaCurrentIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(referenceAssembly + CURRENT_FILE_SUFFIX);
    }

    public static Path getEvaMergedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(referenceAssembly + MERGED_FILE_SUFFIX);
    }

    public static Path getEvaDeprecatedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(referenceAssembly + DEPRECATED_FILE_SUFFIX);
    }

    public static Path getEvaMergedDeprecatedIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(referenceAssembly + MERGED_DEPRECATED_FILE_SUFFIX);
    }

    public static Path getEvaMultimapIdsReportPath(String outputFolder, String referenceAssembly) {
        return Paths.get(outputFolder).resolve(referenceAssembly + MULTIMAP_FILE_SUFFIX);
    }
}
