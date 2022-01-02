/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.functions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.cql3.functions.TimeFcts.toTimestamp;
import static org.junit.Assert.assertEquals;

public class MathFctsTest
{

    @Test
    public void testAbs()
    {
        final ByteBuffer one32 = Int32Type.instance.fromString("1");
        final ByteBuffer zeroByte = ByteType.instance.fromString("0");
        final ByteBuffer negativeThree32 = Int32Type.instance.fromString("-3");
        final NativeScalarFunction abs32Func = MathFcts.absFct(Int32Type.instance);
        final NativeScalarFunction absByteFunc = MathFcts.absFct(ByteType.instance);

        assertEquals(1, ByteBufferUtil.toInt(executeFunction(abs32Func, one32)));
        assertEquals(0, ByteBufferUtil.toByte(executeFunction(absByteFunc, zeroByte)));
        assertEquals(3, ByteBufferUtil.toInt(executeFunction(abs32Func, negativeThree32)));
    }

    @Test
    public void testExp()
    {
        final ByteBuffer oneShort = ShortType.instance.fromString("1");
        final ByteBuffer zeroLong = LongType.instance.fromString("0");
        final ByteBuffer fiveLong = ShortType.instance.fromString("5");
        final NativeScalarFunction expShortFunc = MathFcts.expFct(ShortType.instance);
        final NativeScalarFunction expLongFunc = MathFcts.expFct(LongType.instance);

        assertEquals(Math.E, ByteBufferUtil.toDouble(executeFunction(expShortFunc, oneShort)),0);
        assertEquals(1, ByteBufferUtil.toDouble(executeFunction(expLongFunc, zeroLong)), 0);
        assertEquals(
            Math.pow(Math.E, 5),ByteBufferUtil.toDouble(executeFunction(expShortFunc, fiveLong)), 0.00000001
        );
    }

    @Test
    public void testLog()
    {
        final ByteBuffer zeroShort = ShortType.instance.fromString("0");
        final ByteBuffer eDouble = DoubleType.instance.fromString(String.format("%f", Math.E));
        final ByteBuffer fiveInt = IntegerType.instance.fromString("5");
        final ByteBuffer onePointSixishDecimal = DecimalType.instance.fromString(
        "1.60943791243410037460075933322619"
        );
        final NativeScalarFunction logShortFunc = MathFcts.logFct(ShortType.instance);
        final NativeScalarFunction logDoubleFunc = MathFcts.logFct(DoubleType.instance);
        final NativeScalarFunction logIntFunc = MathFcts.logFct(IntegerType.instance);

        assertEquals(
            Double.NEGATIVE_INFINITY, ByteBufferUtil.toDouble(executeFunction(logShortFunc, zeroShort)), 0
        );
        assertEquals(1, ByteBufferUtil.toDouble(executeFunction(logDoubleFunc, eDouble)), 0.000001);
        assertEquals(onePointSixishDecimal, executeFunction(logIntFunc, fiveInt));
    }

    @Test
    public void testLog10()
    {
        final ByteBuffer onehundredInt = Int32Type.instance.fromString("100");
        final ByteBuffer negativeOneFloat = FloatType.instance.fromString("-1");
        final ByteBuffer sixtyfourDecimal = DecimalType.instance.fromString("64");
        final ByteBuffer onePointEightishDecimal = DecimalType.instance.fromString(
        "1.80617997398388717128243336834696"
        );

        final NativeScalarFunction log10IntFunc = MathFcts.log10Fct(Int32Type.instance);
        final NativeScalarFunction log10FloatFunc = MathFcts.log10Fct(FloatType.instance);
        final NativeScalarFunction log10DecimalFunc = MathFcts.log10Fct(DecimalType.instance);

        assertEquals(2, ByteBufferUtil.toDouble(executeFunction(log10IntFunc, onehundredInt)), 0);
        assertEquals(Double.NaN, ByteBufferUtil.toDouble(executeFunction(log10FloatFunc, negativeOneFloat)), 0);
        assertEquals(onePointEightishDecimal, executeFunction(log10DecimalFunc, sixtyfourDecimal));

    }

    @Test
    public void testRound()
    {
        final ByteBuffer fivePointFiveDecimal = DecimalType.instance.fromString("5.5");
        final ByteBuffer sixInt = IntegerType.instance.fromString("6");
        final ByteBuffer negativeTwoPointNineFloat = FloatType.instance.fromString("-2.9");
        final ByteBuffer fourLong = LongType.instance.fromString("4");
        final NativeScalarFunction roundDecimalFunc = MathFcts.roundFct(DecimalType.instance);
        final NativeScalarFunction roundFloatFunc = MathFcts.roundFct(FloatType.instance);
        final NativeScalarFunction roundLongFunc = MathFcts.roundFct(LongType.instance);

        assertEquals(sixInt, executeFunction(roundDecimalFunc, fivePointFiveDecimal));
        assertEquals(-3, ByteBufferUtil.toInt(executeFunction(roundFloatFunc, negativeTwoPointNineFloat)));
        assertEquals(4, ByteBufferUtil.toLong(executeFunction(roundLongFunc, fourLong)));
    }

    private static ByteBuffer executeFunction(Function function, ByteBuffer input)
    {
        List<ByteBuffer> params = Collections.singletonList(input);
        return ((ScalarFunction) function).execute(ProtocolVersion.CURRENT, params);
    }
}
