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
package uk.ac.ebi.eva.accession.dbsnp2.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * There's no way to read a compressed file with Spring classes.
 * reference: <a href=https://jira.spring.io/browse/BATCH-1750>Jira Spring Issue #1750</a>
 * <p>
 * It's lazy because otherwise it will try to open the file on creation. The creation may be at the start of the
 * runtime if this class is used to create beans for auto-wiring, and at the start of the application it's
 * possible that the file doesn't exist yet.
 */
public class BzipLazyResource extends FileSystemResource {

    public BzipLazyResource(File file) {
        super(file);
    }

    public BzipLazyResource(String path) {
        super(path);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        BufferedInputStream bis = new BufferedInputStream(super.getInputStream());
        try {
            return new CompressorStreamFactory().createCompressorInputStream(bis);
        } catch (CompressorException compressorException) {
            throw new IOException("The input file is not compressed in bzip2 format");
        }
    }
}

