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
package uk.ac.ebi.eva.accession.dbsnp.contig;

import uk.ac.ebi.eva.accession.dbsnp.io.AssemblyReportReader;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContigMapping {

    private Map<String, ContigSynonyms> sequenceNameToSynonyms;

    private Map<String, ContigSynonyms> genBankToSynonyms;

    private Map<String, ContigSynonyms> refSeqToSynonyms;

    private Map<String, ContigSynonyms> ucscToSynonyms;

    private static final String CHROMOSOME_PATTERN = "(chromosome|chrom|chr|ch)(.+)";

    private static final Pattern PATTERN = Pattern.compile(CHROMOSOME_PATTERN, Pattern.CASE_INSENSITIVE);

    public ContigMapping(String mappingUrl) throws Exception {
        this(new AssemblyReportReader(mappingUrl));
    }

    public ContigMapping(AssemblyReportReader assemblyReportReader) throws Exception {
        sequenceNameToSynonyms = new HashMap<>();
        genBankToSynonyms = new HashMap<>();
        refSeqToSynonyms = new HashMap<>();
        ucscToSynonyms = new HashMap<>();
        populateMaps(assemblyReportReader);
    }

    private void populateMaps(AssemblyReportReader assemblyReportReader) throws Exception {
        ContigSynonyms contigSynonyms;
        while ((contigSynonyms = assemblyReportReader.read()) != null) {
            fillContigConventionMaps(contigSynonyms);
        }
    }

    private void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        contigSynonyms.setSequenceName(removePrefix(contigSynonyms.getSequenceName()));
        contigSynonyms.setUcsc(removePrefix(contigSynonyms.getUcsc()));
        sequenceNameToSynonyms.put(contigSynonyms.getSequenceName(), contigSynonyms);
        genBankToSynonyms.put(contigSynonyms.getGenBank(), contigSynonyms);
        refSeqToSynonyms.put(contigSynonyms.getRefSeq(), contigSynonyms);
        ucscToSynonyms.put(contigSynonyms.getUcsc(), contigSynonyms);
    }

    private String removePrefix(String contig) {
        Matcher matcher = PATTERN.matcher(contig);
        String contigNoPrefix = contig;
        if (matcher.matches()) {
            contigNoPrefix = matcher.group(2);
        }
        return contigNoPrefix;
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        String contigNoPrefix = removePrefix(contig);
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = sequenceNameToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = genBankToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = refSeqToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = ucscToSynonyms.get(contigNoPrefix)) != null) {
            return contigSynonyms;
        }
        return null;
    }
}
