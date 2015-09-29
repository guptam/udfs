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

import io.netty.buffer.DrillBuf;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.commons.codec.CharEncoding;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.DecoderFactory;

public class AvroDirectRecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroDirectRecordReader.class);

  private DrillBuf buffer;

  public AvroDirectRecordReader(DrillBuf buffer) {
    this.buffer = buffer;
  }

  private static final Schema DEFAULT_SCHEMA = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"ClassifierPredicate\",\"namespace\":\"com.sift.classifier.avro\",\"fields\":[{\"name\":\"key\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]}");

  /** Deserialize Avro bytes. */
  public static Object fromBinary(Schema schema, byte[] binary) throws IOException {
    DatumReader<Object> reader = new SpecificDatumReader<Object>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(binary, null);
    return reader.read(null, decoder);
  }

  public void processRecord(byte []recordBytes, ComplexWriter writer) {
    try {
      processRecord(fromBinary(DEFAULT_SCHEMA, recordBytes), DEFAULT_SCHEMA, writer);
    } catch (IOException ioe) {
      throw new DrillRuntimeException("Error during process record", ioe);
    }
  }

  private void processRecord(final Object value, final Schema schema, final ComplexWriter writer) {
    final Schema.Type type = schema.getType();
    switch (type) {
      case RECORD:
        process(value, schema, null, new MyMapOrListWriter(writer.rootAsMap()));
        break;
      default:
        throw new DrillRuntimeException("Root object must be record type. Found: " + type);
    }
  }

  private void process(final Object value, final Schema schema, final String fieldName, MyMapOrListWriter writer) {
    if (value == null) {
      return;
    }
    final Schema.Type type = schema.getType();

    switch (type) {
      case RECORD:
        // list field of MapOrListWriter will be non null when we want to store array of maps/records.
        MyMapOrListWriter _writer = writer;

        for (final Schema.Field field : schema.getFields()) {
          if (field.schema().getType() == Schema.Type.RECORD ||
            (field.schema().getType() == Schema.Type.UNION &&
              field.schema().getTypes().get(0).getType() == Schema.Type.NULL &&
              field.schema().getTypes().get(1).getType() == Schema.Type.RECORD)) {
            _writer = writer.map(field.name());
          }

          process(((GenericRecord) value).get(field.name()), field.schema(), field.name(), _writer);
        }
        break;
      case ARRAY:
        assert fieldName != null;
        final GenericArray array = (GenericArray) value;
        Schema elementSchema = array.getSchema().getElementType();
        Schema.Type elementType = elementSchema.getType();
        if (elementType == Schema.Type.RECORD || elementType == Schema.Type.MAP){
          writer = writer.list(fieldName).listoftmap(fieldName);
        } else {
          writer = writer.list(fieldName);
        }
        writer.start();
        for (final Object o : array) {
          process(o, elementSchema, fieldName, writer);
        }
        writer.end();
        break;
      case UNION:
        // currently supporting only nullable union (optional fields) like ["null", "some-type"].
        if (schema.getTypes().get(0).getType() != Schema.Type.NULL) {
          throw new UnsupportedOperationException("Avro union type must be of the format : [\"null\", \"some-type\"]");
        }
        process(value, schema.getTypes().get(1), fieldName, writer);
        break;
      case MAP:
        final HashMap<Object, Object> map = (HashMap<Object, Object>) value;
        Schema valueSchema = schema.getValueType();
        writer = writer.map(fieldName);
        writer.start();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          process(entry.getValue(), valueSchema, entry.getKey().toString(), writer);
        }
        writer.end();
        break;
      case FIXED:
        throw new UnsupportedOperationException("Unimplemented type: " + type.toString());
      case ENUM:  // Enum symbols are strings
      case NULL:  // Treat null type as a primitive
      default:
        assert fieldName != null;
        processPrimitive(value, schema.getType(), fieldName, writer);
        break;
    }
  }

  private void processPrimitive(final Object value, final Schema.Type type, final String fieldName,
                                final MyMapOrListWriter writer) {
    if (value == null) {
      return;
    }
    switch (type) {
      case STRING:
        byte[] binary = null;
        if (value instanceof Utf8) {
          binary = ((Utf8) value).getBytes();
        } else {
          try {
            binary = value.toString().getBytes(CharEncoding.UTF_8);
          } catch (UnsupportedEncodingException upe) {
            throw new DrillRuntimeException("error encoding utf8 ", upe);
          }
        }
        final int length = binary.length;
        final VarCharHolder vh = new VarCharHolder();
        ensure(length);
        buffer.setBytes(0, binary);
        vh.buffer = buffer;
        vh.start = 0;
        vh.end = length;
        writer.varChar(fieldName).write(vh);
        break;
      case INT:
        final IntHolder ih = new IntHolder();
        ih.value = (Integer) value;
        writer.integer(fieldName).write(ih);
        break;
      case LONG:
        final BigIntHolder bh = new BigIntHolder();
        bh.value = (Long) value;
        writer.bigInt(fieldName).write(bh);
        break;
      case FLOAT:
        final Float4Holder fh = new Float4Holder();
        fh.value = (Float) value;
        writer.float4(fieldName).write(fh);
        break;
      case DOUBLE:
        final Float8Holder f8h = new Float8Holder();
        f8h.value = (Double) value;
        writer.float8(fieldName).write(f8h);
        break;
      case BOOLEAN:
        final BitHolder bit = new BitHolder();
        bit.value = (Boolean) value ? 1 : 0;
        writer.bit(fieldName).write(bit);
        break;
      case BYTES:
        // XXX - Not sure if this is correct. Nothing prints from sqlline for byte fields.
        final VarBinaryHolder vb = new VarBinaryHolder();
        final ByteBuffer buf = (ByteBuffer) value;
        final byte[] bytes = buf.array();
        ensure(bytes.length);
        buffer.setBytes(0, bytes);
        vb.buffer = buffer;
        vb.start = 0;
        vb.end = bytes.length;
        writer.binary(fieldName).write(vb);
        break;
      case NULL:
        // Nothing to do for null type
        break;
      case ENUM:
        final String symbol = value.toString();
        final byte[] b;
        try {
          b = symbol.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
          throw new DrillRuntimeException("Unable to read enum value for field: " + fieldName, e);
        }
        final VarCharHolder vch = new VarCharHolder();
        ensure(b.length);
        buffer.setBytes(0, b);
        vch.buffer = buffer;
        vch.start = 0;
        vch.end = b.length;
        writer.varChar(fieldName).write(vch);
        break;
      default:
        throw new DrillRuntimeException("Unhandled Avro type: " + type.toString());
    }
  }

  private void ensure(final int length) {
    buffer = buffer.reallocIfNeeded(length);
  }
}
