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
package com.google.appengine.datanucleus;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

/**
 * Static utility methods pertaining to {@code long} primitives, that are not
 * already found in either {@link Long} or {@link Arrays}.
 *
 * @author Kevin Bourrillion
 */
final class Longs {
  private Longs() {}

  /**
   * Returns a hash code for {@code value}; equal to the result of invoking
   * {@code ((Long) value).hashCode()}.
   *
   * @param value a primitive {@code long} value
   * @return a hash code for the value
   */
  private static int hashCode(long value) {
    return (int) (value ^ (value >>> 32));
  }

  // TODO(kevinb): consider making this public
  private static int indexOf(long target, long[] array, int start, int end) {
    for (int i = start; i < end; i++) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }

  // TODO(kevinb): consider making this public
  private static int lastIndexOf(
      long target, long[] array, int start, int end) {
    for (int i = end - 1; i >= start; i--) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Copies a collection of {@code Long} instances into a new array of
   * primitive {@code long} values.
   *
   * @param collection a collection of {@code Long} objects
   * @return an array containing the same values as {@code collection}, in the
   *     same order, converted to primitives
   * @throws NullPointerException if {@code collection} or any of its elements
   *     are null
   */
  static long[] toArray(Collection<Long> collection) {
    if (collection instanceof LongArrayAsList) {
      return ((LongArrayAsList) collection).toLongArray();
    }

    // TODO(kevinb): handle collection being concurrently modified
    int counter = 0;
    long[] array = new long[collection.size()];
    for (Long value : collection) {
      array[counter++] = value;
    }
    return array;
  }

  /**
   * Returns a fixed-size list backed by the specified array, similar to {@link
   * Arrays#asList(Object[])}. The list supports {@link List#set(int, Object)},
   * but any attempt to set a value to {@code null} will result in a {@link
   * NullPointerException}.
   *
   * <p>The returned list maintains the values, but not the identities, of
   * {@code Long} objects written to or read from it.  For example, whether
   * {@code list.get(0) == list.get(0)} is true for the returned list is
   * unspecified.
   *
   * @param backingArray the array to back the list
   * @return a list view of the array
   */
  static List<Long> asList(long... backingArray) {
    if (backingArray.length == 0) {
      return Collections.emptyList();
    }
    return new LongArrayAsList(backingArray);
  }

  private static class LongArrayAsList extends AbstractList<Long>
      implements RandomAccess, Serializable {
    final long[] array;
    final int start;
    final int end;

    LongArrayAsList(long[] array) {
      this(array, 0, array.length);
    }

    LongArrayAsList(long[] array, int start, int end) {
      this.array = array;
      this.start = start;
      this.end = end;
    }

    @Override public int size() {
      return end - start;
    }

    @Override public boolean isEmpty() {
      return false;
    }

    @Override public Long get(int index) {
      return array[start + index];
    }

    @Override public boolean contains(Object target) {
      // Overridden to prevent a ton of boxing
      return (target instanceof Long)
          && Longs.indexOf((Long) target, array, start, end) != -1;
    }

    @Override public int indexOf(Object target) {
      // Overridden to prevent a ton of boxing
      if (target instanceof Long) {
        int i = Longs.indexOf((Long) target, array, start, end);
        if (i >= 0) {
          return i - start;
        }
      }
      return -1;
    }

    @Override public int lastIndexOf(Object target) {
      // Overridden to prevent a ton of boxing
      if (target instanceof Long) {
        int i = Longs.lastIndexOf((Long) target, array, start, end);
        if (i >= 0) {
          return i - start;
        }
      }
      return -1;
    }

    @Override public Long set(int index, Long element) {
      long oldValue = array[start + index];
      array[start + index] = element;
      return oldValue;
    }

    @Override public List<Long> subList(int fromIndex, int toIndex) {
      if (fromIndex == toIndex) {
        return Collections.emptyList();
      }
      return new LongArrayAsList(array, start + fromIndex, start + toIndex);
    }

    @Override public boolean equals(Object object) {
      if (object == this) {
        return true;
      }
      if (object instanceof LongArrayAsList) {
        LongArrayAsList that = (LongArrayAsList) object;
        int size = size();
        if (that.size() != size) {
          return false;
        }
        for (int i = 0; i < size; i++) {
          if (array[start + i] != that.array[that.start + i]) {
            return false;
          }
        }
        return true;
      }
      return super.equals(object);
    }

    @Override public int hashCode() {
      int result = 1;
      for (int i = start; i < end; i++) {
        result = 31 * result + Longs.hashCode(array[i]);
      }
      return result;
    }

    @Override public String toString() {
      StringBuilder builder = new StringBuilder(size() * 10);
      builder.append('[').append(array[start]);
      for (int i = start + 1; i < end; i++) {
        builder.append(", ").append(array[i]);
      }
      return builder.append(']').toString();
    }

    long[] toLongArray() {
      // Arrays.copyOfRange() requires Java 6
      int size = size();
      long[] result = new long[size];
      System.arraycopy(array, start, result, 0, size);
      return result;
    }

    private static final long serialVersionUID = 0;
  }
}
