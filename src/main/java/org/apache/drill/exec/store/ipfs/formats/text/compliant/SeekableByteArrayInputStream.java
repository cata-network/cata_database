/**
 * Copyright 2013 Knewton
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.drill.exec.store.ipfs.formats.text.compliant;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Helper input stream which composes a {@link ByteArrayInputStream} and implements {@link Seekable}
 * and {@link PositionedReadable} for testing the {@link JsonRecordReader}
 *
 */
public class SeekableByteArrayInputStream extends InputStream implements Seekable,
    PositionedReadable {

  private ByteArrayInputStream inputStream;
  private long pos;

  public SeekableByteArrayInputStream(byte[] recommendationBytes) {
    this.inputStream = new ByteArrayInputStream(recommendationBytes);
    pos = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void seek(long pos) throws IOException {
    inputStream.skip(pos);
    this.pos = pos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getPos() throws IOException {
    return pos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException("seekToNewSource is not supported.");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read() throws IOException {
    int val = inputStream.read();
    if (val > 0) {
      pos++;
    }
    return val;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    long currPos = pos;
    inputStream.skip(position);
    int bytesRead = inputStream.read(buffer, offset, length);
    inputStream.skip(currPos);
    return bytesRead;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    long currPos = pos;
    inputStream.skip(position);
    inputStream.read(buffer, offset, length);
    inputStream.skip(currPos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    long currPos = pos;
    inputStream.skip(position);
    inputStream.read(buffer);
    inputStream.skip(currPos);
  }

}