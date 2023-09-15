/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.kite;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.vitessedata.kite.sdk.KiteConnection;
import com.vitessedata.xrg.format.ArrayType;
import com.vitessedata.xrg.format.XrgIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/*
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
*/
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
//import static com.google.common.base.Preconditions.checkState;

public class KiteRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(KiteRecordCursor.class);

    private final List<KiteColumnHandle> columnHandles;
    private final KiteConnection kite;
    private Object[] values;
    private byte[] flags;
    private int count;

    public KiteRecordCursor(KiteConnection kite, List<KiteColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        this.kite = kite;
        this.values = null;
        this.flags = null;
        this.count = 0;
        try {
            this.kite.submit();
        }
        catch (IOException e) {
            throw new PrestoException(REMOTE_HOST_GONE, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            XrgIterator iter = kite.next();
            if (iter == null) {
                return false;
            }

            values = iter.getValues();
            flags = iter.getFlags();
            count++;
        }
        catch (IOException e) {
            throw new PrestoException(REMOTE_TASK_ERROR, e);
        }
        return true;
    }

    private String getFieldValue(int field)
    {
        Object value = values[field];
        if (value instanceof String) {
            return (String) values[field];
        }
        else {
            throw new IllegalStateException("Expected Long but " + values[field].getClass().getName());
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        checkArgument((values[field] instanceof Byte), "Expected Byte but %s", values[field].getClass().getName());
        return (((Byte) values[field]).byteValue() != 0);
    }

    @Override
    public long getLong(int field)
    {
        Type type = getType(field);
        Object value = values[field];
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            long v = ((Number) value).longValue();
            if (type.equals(TIMESTAMP) || type.equals(TIME)) {
                v /= 1000;
            }
            return v;
        }
        else if (value instanceof BigInteger) {
            return ((BigInteger) value).longValueExact();
        }
        else if (value instanceof BigDecimal) {
            BigDecimal v = (BigDecimal) value;
            return v.unscaledValue().longValueExact(); // return the unscale value. e.g. 0.110 (scale=3) => 110
        }
        else if (value instanceof Float) {
            Float f = (Float) value;
            return Float.floatToRawIntBits(f.floatValue());
        }
        else if (value instanceof Double) {
            Double d = (Double) value;
            return Double.doubleToRawLongBits(d.doubleValue());
        }
        else {
            throw new IllegalStateException("Expected Long but " + values[field].getClass().getName());
        }
    }

    @Override
    public double getDouble(int field)
    {
        Object value = values[field];
        if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
            return ((Number) value).doubleValue();
        }
        else {
            throw new IllegalStateException("Expected Double but " + values[field].getClass().getName());
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        Object value = values[field];
        if (value instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) values[field]);
        }
        else if (value instanceof String) {
            return Slices.utf8Slice((String) value);
        }
        else {
            throw new IllegalStateException("Slice: Expected byte[] or String but " + values[field].getClass().getName());
        }
    }

    private void writeNativeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type.getJavaType() == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (type.getJavaType() == long.class) {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                long v = ((Number) value).longValue();
                if (type.equals(TIMESTAMP) || type.equals(TIME)) {
                    v /= 1000;
                }
                type.writeLong(blockBuilder, v);
            }
            else if (value instanceof BigInteger) {
                type.writeLong(blockBuilder, ((BigInteger) value).longValueExact());
            }
            else if (value instanceof BigDecimal) {
                BigDecimal v = (BigDecimal) value;
                type.writeLong(blockBuilder, v.unscaledValue().longValueExact()); // return the unscale value. e.g. 0.110 (scale=3) => 110
            }
            else if (value instanceof Float) {
                Float f = (Float) value;
                type.writeLong(blockBuilder, Float.floatToRawIntBits(f.floatValue()));
            }
            else if (value instanceof Double) {
                Double d = (Double) value;
                type.writeLong(blockBuilder, Double.doubleToRawLongBits(d.doubleValue()));
            }
        }
        else if (type.getJavaType() == double.class) {
            if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
        }
        else if (type.getJavaType() == Slice.class) {
            Slice slice;
            if (value instanceof byte[]) {
                slice = Slices.wrappedBuffer((byte[]) value);
            }
            else if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else {
                throw new IllegalStateException("Slice expected byte[] or String but " + value.getClass().getName() + " type = " + type.getDisplayName());
            }
            type.writeSlice(blockBuilder, slice, 0, slice.length());
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        Object value = values[field];
        if (value instanceof ArrayType && Types.isArrayType(type)) {
            ArrayType arr = (ArrayType) value;
            Object[] objs = arr.toArray();
            Type elementType = Types.getElementType(type);
            BlockBuilder builder = elementType.createBlockBuilder(null, objs.length);
            for (int i = 0; i < objs.length; i++) {
                writeNativeValue(elementType, builder, objs[i]);
            }
            return builder.build();
        }
        else {
            throw new IllegalStateException("Expected ArrayType but " + values[field].getClass().getName());
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return (flags[field] != 0);
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        try {
            kite.release();
        }
        catch (IOException e) {
        }
    }
}
