/**********************************************************************
Copyright (c) 2009 Google Inc.

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
package com.google.appengine.datanucleus.query;

import junit.framework.TestCase;

import com.google.appengine.datanucleus.Utils;

import java.util.Collections;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class SlicingIterableTest extends TestCase {

  public void testNoDataZeroOffsetNoLimit() {
    assertNoData(new SlicingIterable<Integer>(0, null, Collections.<Integer>emptyList()));
  }

  public void testOneElementZeroOffsetNoLimit() {
    List<Integer> list = Utils.newArrayList(1);
    assertEquals(list, new SlicingIterable<Integer>(0, null, list));
  }

  public void testMultipleElementsZeroOffsetNoLimit() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertEquals(list, new SlicingIterable<Integer>(0, null, list));
  }

  public void testMultipleElementsZeroOffsetZeroLimit() {
    assertNoData(new SlicingIterable<Integer>(0, 0, Utils.newArrayList(1, 2, 3)));
  }

  public void testMultipleElementsZeroOffsetLimitGreaterThanInputSize() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertEquals(list, new SlicingIterable<Integer>(0, list.size() + 1, list));
  }

  public void testMultipleElementsZeroOffsetLimitEqualToInputSize() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertEquals(list, new SlicingIterable<Integer>(0, list.size(), list));
  }

  public void testMultipleElementsZeroOffsetLimitSmallerThanInputSize() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertEquals(list.subList(0, list.size() - 1), new SlicingIterable<Integer>(0, list.size() - 1, list));
    assertEquals(list.size() - 1, iterableToList(new SlicingIterable<Integer>(0, list.size() - 1, list)).size());
    assertEquals(list.subList(0, list.size() - 2), new SlicingIterable<Integer>(0, list.size() - 2, list));
    assertEquals(list.size() - 2, iterableToList(new SlicingIterable<Integer>(0, list.size() - 2, list)).size());
  }

  public void testMultipleElementsOffsetGreaterThanInputSizeNoLimit() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertNoData(new SlicingIterable<Integer>(list.size() + 1, null, list));
  }

  public void testMultipleElementsOffsetEqualToInputSizeNoLimit() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertNoData(new SlicingIterable<Integer>(list.size(), null, list));
  }

  public void testMultipleElementsOffsetLessThanInputSizeNoLimit() {
    List<Integer> list = Utils.newArrayList(1, 2, 3);
    assertEquals(list.subList(2, list.size()), new SlicingIterable<Integer>(list.size() - 1, null, list));
    assertEquals(list.subList(1, list.size()), new SlicingIterable<Integer>(list.size() - 2, null, list));
  }

  public void testOffsetAndLimit() {
    List<Integer> list = Utils.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertEquals(list.subList(1, list.size()), new SlicingIterable<Integer>(1, list.size(), list));
    for (int offset = 0; offset < 10; offset++) {
      for (int limit = 10; limit >= 0; limit--) {
        assertEquals("Failed with offset " + offset + " and limit " + limit,
                     list.subList(offset, Math.min(offset + limit, list.size())),
                     new SlicingIterable<Integer>(offset, limit, list));
      }
    }
  }

  private static void assertEquals(String failureMsg, List<Integer> expected, SlicingIterable<Integer> actual) {
    List<Integer> actualList = iterableToList(actual);
    assertEquals(expected, actualList);
    // do it again to make sure we get the same answer
    actualList = iterableToList(actual);
    assertEquals(failureMsg, expected, actualList);
  }

  private static void assertEquals(List<Integer> expected, SlicingIterable<Integer> actual) {
    List<Integer> actualList = iterableToList(actual);
    assertEquals(expected, actualList);
    // do it again to make sure we get the same answer
    actualList = iterableToList(actual);
    assertEquals(expected, actualList);
  }

  private static List<Integer> iterableToList(SlicingIterable<Integer> iterable) {
    List<Integer> list = Utils.newArrayList();
    for (Integer i : iterable) {
      list.add(i);
    }
    return list;
  }

  private static void assertNoData(Iterable<?> iterable) {
    assertFalse(iterable.iterator().hasNext());
    // do it again to make sure we get the same answer
    assertFalse(iterable.iterator().hasNext());
  }
}