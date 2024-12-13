package com.ethlo.kfka.compression;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2023 Morten Haraldsen (ethlo)
 * %%
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
 * #L%
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipPayloadCompressor implements PayloadCompressor
{
    @Override
    public byte[] compress(final byte[] uncompressed)
    {
        final int length = uncompressed.length;
        try (final ByteArrayOutputStream bout = new ByteArrayOutputStream(length);
             final DataOutputStream dataOutputStream = new DataOutputStream(bout);
        )
        {
            dataOutputStream.writeInt(length);
            dataOutputStream.flush();
            dataOutputStream.close();

            final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bout);
            gzipOutputStream.write(uncompressed);
            gzipOutputStream.flush();
            gzipOutputStream.close();

            return bout.toByteArray();
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
    }

    @Override
    public byte[] decompress(final byte[] compressed)
    {
        try
        {
            final int length = new DataInputStream(new ByteArrayInputStream(compressed)).readInt();
            final ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
            if (bin.skip(4) != 4)
            {
                throw new UncheckedIOException(new IOException("Unable to skip 4 bytes"));
            }
            return new GZIPInputStream(bin, length).readAllBytes();
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
    }
}
