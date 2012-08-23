/**********************************************************************
Copyright (c) 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 **********************************************************************/
package com.google.appengine.datanucleus;

import junit.framework.TestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

/**
 * Tests for {@link BigDecimals}
 *
 * @author tkaitchuck@google.com (Tom Kaitchuck)
 */
public class BigDecimalsTest extends TestCase {

  public void testZerosAreEqual() {
    BigDecimal zero = new BigDecimal(0);
    BigDecimal otherZero = new BigDecimal(new BigInteger("00000"), 100);
    String zeroString = BigDecimals.toSortableString(zero);
    String otherZeroString = BigDecimals.toSortableString(otherZero);
    assertEquals(zeroString, otherZeroString);
  }
  
  public void testOnesAreEqual() {
    BigDecimal one = new BigDecimal(1);
    BigDecimal otherOne = new BigDecimal(new BigInteger("10000"), 4);
    String oneString = BigDecimals.toSortableString(one);
    String otherOneString = BigDecimals.toSortableString(otherOne);
    assertEquals(oneString, otherOneString);
  }
  
  public void testNoExtraDecimals() {
    BigDecimal one = new BigDecimal(1);
    String oneString = BigDecimals.toSortableString(one);
    assertEquals(one, BigDecimals.fromSortableString(oneString));
  }

  public void testZeroPadding() {
    BigDecimal zero = new BigDecimal(100);
    BigDecimal otherZero = new BigDecimal(new BigInteger("00001000"), 1);
    String zeroString = BigDecimals.toSortableString(zero);
    String otherZeroString = BigDecimals.toSortableString(otherZero);
    assertEquals(zeroString, otherZeroString);
  }

  public void testExpectedFixedLength() {
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), -10);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), 10);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), 10);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), -10);
    assertEquals(BigDecimals.STRING_SIZE,
        BigDecimals.toSortableString(veryNegitive).length());
    assertEquals(BigDecimals.STRING_SIZE,
        BigDecimals.toSortableString(somewhatNegitive).length());
    assertEquals(BigDecimals.STRING_SIZE,
        BigDecimals.toSortableString(somewhatPositive).length());
    assertEquals(BigDecimals.STRING_SIZE,
        BigDecimals.toSortableString(veryPositive).length());
  }

  public void testExpectedFormat() {
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), -0x1000);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), 0x1000);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), 0x1000);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), -0x1000);
    assertTrue(
        BigDecimals.toSortableString(veryNegitive).startsWith("-+fffff062,f"));
    assertTrue(
        BigDecimals.toSortableString(somewhatNegitive).startsWith("--00001062,f"));
    assertTrue(
        BigDecimals.toSortableString(somewhatPositive).startsWith("_-ffffef9e,0"));
    assertTrue(
        BigDecimals.toSortableString(veryPositive).startsWith("__00000f9e,0"));
  }

  public void testExpectedSortOrder() {
    BigDecimal minValue = new BigDecimal(BigInteger.valueOf(-1), Integer.MIN_VALUE);
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), -10);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), 10);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), 10);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), -10);
    BigDecimal nearMaxValue = new BigDecimal(new BigInteger(createMaxValue()), Integer.MIN_VALUE+1);
    BigDecimal maxValue = new BigDecimal(new BigInteger(createMaxValue()), Integer.MIN_VALUE);
    String s0 = BigDecimals.toSortableString(minValue);
    String s1 = BigDecimals.toSortableString(veryNegitive);
    String s2 = BigDecimals.toSortableString(somewhatNegitive);
    String s3 = BigDecimals.toSortableString(somewhatPositive);
    String s4 = BigDecimals.toSortableString(veryPositive);
    String s5 = BigDecimals.toSortableString(nearMaxValue);
    String s6 = BigDecimals.toSortableString(maxValue);
    assertTrue(s0.compareTo(s1) < 0);
    assertTrue(s0.compareTo(s2) < 0);
    assertTrue(s0.compareTo(s3) < 0);
    assertTrue(s0.compareTo(s4) < 0);
    assertTrue(s0.compareTo(s5) < 0);
    assertTrue(s0.compareTo(s6) < 0);
    
    assertTrue(s1.compareTo(s2) < 0);
    assertTrue(s1.compareTo(s3) < 0);
    assertTrue(s1.compareTo(s4) < 0);
    assertTrue(s2.compareTo(s1) > 0);
    assertTrue(s2.compareTo(s3) < 0);
    assertTrue(s2.compareTo(s4) < 0);
    assertTrue(s3.compareTo(s1) > 0);
    assertTrue(s3.compareTo(s2) > 0);
    assertTrue(s3.compareTo(s4) < 0);
    assertTrue(s4.compareTo(s1) > 0);
    assertTrue(s4.compareTo(s2) > 0);
    assertTrue(s4.compareTo(s3) > 0);
    
    assertTrue(s5.compareTo(s0) > 0);
    assertTrue(s5.compareTo(s1) > 0);
    assertTrue(s5.compareTo(s2) > 0);
    assertTrue(s5.compareTo(s3) > 0);
    assertTrue(s5.compareTo(s4) > 0);
    assertTrue(s6.compareTo(s5) > 0);
  }

  public void testExpectedSortOrderPositiveSigned() {
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), 9);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), 10);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), 10);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), 9);
    String s1 = BigDecimals.toSortableString(veryNegitive);
    String s2 = BigDecimals.toSortableString(somewhatNegitive);
    String s3 = BigDecimals.toSortableString(somewhatPositive);
    String s4 = BigDecimals.toSortableString(veryPositive);
    assertTrue(s1.compareTo(s2) < 0);
    assertTrue(s1.compareTo(s3) < 0);
    assertTrue(s1.compareTo(s4) < 0);
    assertTrue(s2.compareTo(s1) > 0);
    assertTrue(s2.compareTo(s3) < 0);
    assertTrue(s2.compareTo(s4) < 0);
    assertTrue(s3.compareTo(s1) > 0);
    assertTrue(s3.compareTo(s2) > 0);
    assertTrue(s3.compareTo(s4) < 0);
    assertTrue(s4.compareTo(s1) > 0);
    assertTrue(s4.compareTo(s2) > 0);
    assertTrue(s4.compareTo(s3) > 0);
  }

  public void testExpectedSortOrderNegitiveSigned() {
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), -10);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), -9);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), -9);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), -10);
    String s1 = BigDecimals.toSortableString(veryNegitive);
    String s2 = BigDecimals.toSortableString(somewhatNegitive);
    String s3 = BigDecimals.toSortableString(somewhatPositive);
    String s4 = BigDecimals.toSortableString(veryPositive);
    assertTrue(s1.compareTo(s2) < 0);
    assertTrue(s1.compareTo(s3) < 0);
    assertTrue(s1.compareTo(s4) < 0);
    assertTrue(s2.compareTo(s1) > 0);
    assertTrue(s2.compareTo(s3) < 0);
    assertTrue(s2.compareTo(s4) < 0);
    assertTrue(s3.compareTo(s1) > 0);
    assertTrue(s3.compareTo(s2) > 0);
    assertTrue(s3.compareTo(s4) < 0);
    assertTrue(s4.compareTo(s1) > 0);
    assertTrue(s4.compareTo(s2) > 0);
    assertTrue(s4.compareTo(s3) > 0);
  }

  public void testRoundTrip() {
    BigDecimal veryNegitive = new BigDecimal(BigInteger.valueOf(-10), -10);
    BigDecimal somewhatNegitive = new BigDecimal(BigInteger.valueOf(-10), 10);
    BigDecimal somewhatPositive = new BigDecimal(BigInteger.valueOf(10), 10);
    BigDecimal veryPositive = new BigDecimal(BigInteger.valueOf(10), -10);
    assertTrue(veryNegitive.compareTo(BigDecimals.fromSortableString(
        BigDecimals.toSortableString(veryNegitive))) == 0);
    assertTrue(somewhatNegitive.compareTo(BigDecimals.fromSortableString(
        BigDecimals.toSortableString(somewhatNegitive))) == 0);
    assertTrue(somewhatPositive.compareTo(BigDecimals.fromSortableString(
        BigDecimals.toSortableString(somewhatPositive))) == 0);
    assertTrue(veryPositive.compareTo(BigDecimals.fromSortableString(
        BigDecimals.toSortableString(veryPositive))) == 0);
  }

  public void testRandomValuesSortOrder() {
    Random r = new Random(0);
    BigDecimal[] decimals = new BigDecimal[10000];
    String[] strings = new String[10000];
    for (int i = 0; i < decimals.length; i++) {
      decimals[i] = createRandomBigDecimal(r, false);
      strings[i] = BigDecimals.toSortableString(decimals[i]);
    }
    Arrays.sort(decimals);
    Arrays.sort(strings);
    for (int i = 0; i < decimals.length; i++) {
      BigDecimal reCreated = BigDecimals.fromSortableString(strings[i]);
      assertTrue(decimals[i].compareTo(reCreated) == 0);
    }
  }

  public void testRandomValuesSortOrderFixedExponent() {
    Random r = new Random(0);
    BigDecimal[] decimals = new BigDecimal[10000];
    String[] strings = new String[10000];
    BigDecimal[] recreated = new BigDecimal[10000];
    for (int i = 0; i < decimals.length; i++) {
      decimals[i] = createRandomBigDecimal(r, true);
      strings[i] = BigDecimals.toSortableString(decimals[i]);
    }
    Arrays.sort(decimals);
    Arrays.sort(strings);
    for (int i = 0; i < decimals.length; i++) {
      recreated[i] = BigDecimals.fromSortableString(strings[i]);
    }
    for (int i = 0; i < decimals.length; i++) {
      assertTrue(decimals[i].compareTo(recreated[i]) == 0);
    }
  }
  
  private BigDecimal createRandomBigDecimal(Random r, boolean fixedExponent) {
    StringBuffer sb = new StringBuffer(BigDecimals.MAX_PRECISION);
    for (int i = 0; i < BigDecimals.MAX_PRECISION; i++) {
      sb.append(r.nextInt(10));
    }
    int scale;
    if (fixedExponent) {
      scale = 0;
    } else {
      scale = r.nextInt();
    }
    BigDecimal result = new BigDecimal(
        new BigInteger(sb.toString()), scale);
    return r.nextBoolean() ? result : result.negate();
  }

  public void testSizeLimits() {

  }

  public void testBoundryNumbers() {
    String maxValue = createMaxValue();
    BigDecimal maxScale = new BigDecimal(new BigInteger(maxValue), Integer.MAX_VALUE);
    testRoundTripNumber(maxScale);
    BigDecimal zeroScale = new BigDecimal(new BigInteger(maxValue), 0);
    testRoundTripNumber(zeroScale);
    BigDecimal minScale = new BigDecimal(new BigInteger(maxValue), Integer.MIN_VALUE);
    testRoundTripNumber(minScale);

    maxScale = new BigDecimal(new BigInteger("1"), Integer.MAX_VALUE-100);
    testRoundTripNumber(maxScale);
    zeroScale = new BigDecimal(new BigInteger("1"), 0);
    testRoundTripNumber(zeroScale);
    minScale = new BigDecimal(new BigInteger("1"), Integer.MIN_VALUE);
    testRoundTripNumber(minScale);
  }

  private String createMaxValue() {
    StringBuffer sb = new StringBuffer(BigDecimals.MAX_PRECISION);
    for (int i = 0; i < BigDecimals.MAX_PRECISION; i++) {
      sb.append(9);
    }
    String maxValue = sb.toString();
    return maxValue;
  }

  private void testRoundTripNumber(BigDecimal toTest) {
    String asString = BigDecimals.toSortableString(toTest);
    assertEquals(BigDecimals.STRING_SIZE, asString.length());
    assertTrue(toTest.compareTo(BigDecimals.fromSortableString(asString)) == 0);
  }

}
