/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration;

public class BeanNames {

    public static final String VCF_READER = "VCF_READER";

    public static final String CLUSTERED_VARIANTS_MONGO_READER = "CLUSTERED_VARIANTS_MONGO_READER";

    public static final String NON_CLUSTERED_VARIANTS_MONGO_READER = "NON_CLUSTERED_VARIANTS_MONGO_READER";

    public static final String RS_MERGE_CANDIDATES_READER = "RS_MERGE_CANDIDATES_READER";

    public static final String RS_SPLIT_CANDIDATES_READER = "RS_SPLIT_CANDIDATES_READER";

    public static final String RS_MERGE_WRITER = "RS_MERGE_WRITER";

    public static final String RS_SPLIT_WRITER = "RS_SPLIT_WRITER";

    public static final String VARIANT_TO_SUBMITTED_VARIANT_ENTITY_PROCESSOR =
            "VARIANT_TO_SUBMITTED_VARIANT_ENTITY_PROCESSOR";

    public static final String NON_CLUSTERED_CLUSTERING_WRITER = "NON_CLUSTERED_CLUSTERING_WRITER";

    public static final String CLUSTERED_CLUSTERING_WRITER = "CLUSTERED_CLUSTERING_WRITER";

    public static final String PROGRESS_LISTENER = "PROGRESS_LISTENER";

    public static final String CLUSTERING_FROM_VCF_STEP = "CLUSTERING_FROM_VCF_STEP";

    public static final String CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP = "CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP";

    public static final String PROCESS_RS_MERGE_CANDIDATES_STEP = "PROCESS_RS_MERGE_CANDIDATES_STEP";

    public static final String PROCESS_RS_SPLIT_CANDIDATES_STEP = "PROCESS_RS_SPLIT_CANDIDATES_STEP";

    public static final String CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP = "CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP";

    public static final String CLUSTERING_FROM_VCF_JOB = "CLUSTERING_FROM_VCF_JOB";

    public static final String CLUSTERING_FROM_MONGO_JOB = "CLUSTERING_FROM_MONGO_JOB";

}
