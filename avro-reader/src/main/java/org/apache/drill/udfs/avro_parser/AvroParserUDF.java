/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.udfs.avro_parser;

import javax.inject.Inject;

import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import io.netty.buffer.DrillBuf;


public class AvroParserUDF {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroParser.class);

  @FunctionTemplate(names = {"avro_parse"},
    scope = org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope.SIMPLE,
    nulls= org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class AvroParser implements org.apache.drill.exec.expr.DrillSimpleFunc {
    @Param VarBinaryHolder input;

    @Output ComplexWriter writer;

    @Inject DrillBuf buffer;
    @Workspace AvroDirectRecordReader reader;

    public void setup() {
      reader = new org.apache.drill.udfs.avro_parser.AvroDirectRecordReader(buffer);
    }

    public void eval() {
      // Read in the input.
      byte[] buf = new byte[input.end - input.start];
      input.buffer.getBytes(input.start, buf, 0, input.end - input.start);
      reader.processRecord(buf, writer);
    }
  }
}
