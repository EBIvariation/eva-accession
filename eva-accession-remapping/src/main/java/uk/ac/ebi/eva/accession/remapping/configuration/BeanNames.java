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
package uk.ac.ebi.eva.accession.remapping.configuration;

public class BeanNames {

    public static final String EXPORT_SUBMITTED_VARIANTS_JOB = "EXPORT_SUBMITTED_VARIANTS_JOB";

    public static final String EXPORT_EVA_SUBMITTED_VARIANTS_STEP = "EXPORT_EVA_SUBMITTED_VARIANTS_STEP";

    public static final String EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP = "EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP";

    public static final String EVA_SUBMITTED_VARIANT_READER = "EVA_SUBMITTED_VARIANT_READER";

    public static final String DBSNP_SUBMITTED_VARIANT_READER = "DBSNP_SUBMITTED_VARIANT_READER";

    public static final String SUBMITTED_VARIANT_PROCESSOR = "SUBMITTED_VARIANT_PROCESSOR";

    public static final String EVA_SUBMITTED_VARIANT_WRITER = "EVA_SUBMITTED_VARIANT_WRITER";

    public static final String DBSNP_SUBMITTED_VARIANT_WRITER = "DBSNP_SUBMITTED_VARIANT_WRITER";

    public static final String EXCLUDE_VARIANTS_LISTENER = "EXCLUDE_VARIANTS_LISTENER";

    public static final String PROGRESS_LISTENER = "PROGRESS_LISTENER";
}
