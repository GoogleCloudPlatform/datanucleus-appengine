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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * A handful of utilities based on the Google Collections Library.
 * Ultimately I'd like to just bundle this with the ORM but at the moment I
 * don't want to introduce any more dependencies than absolutely necessary.
 *
 * @author Max Ross <maxr@google.com>
 */
public class Utils {

  private Utils() {}

  public static <T> ArrayList<T> newArrayList(T... elements) {
    return new ArrayList<T>(Arrays.asList(elements));
  }

  public static <T> LinkedList<T> newLinkedList(T... elements) {
    return new LinkedList<T>(Arrays.asList(elements));
  }

  public static <T> HashSet<T> newHashSet(T... elements) {
    return new HashSet<T>(Arrays.asList(elements));
  }

  public static <T> HashSet<T> newHashSet(Collection<T> elements) {
    return new HashSet<T>(elements);
  }

  public static <T> TreeSet<T> newTreeSet(T... elements) {
    return new TreeSet<T>(Arrays.asList(elements));
  }

  public static <F, T> List<T> transform(Iterable<F> from, Function<? super F, ? extends T> func) {
    List<T> to = new ArrayList<T>();
    for (F fromElement : from) {
      to.add(func.apply(fromElement));
    }
    return to;
  }

  public static <T> Function<T, T> identity() {
    return new Function<T, T>() {
      public T apply(T from) {
        return from;
      }
    };
  }

  public static <K, V> HashMap<K, V> newHashMap() {
    return new HashMap<K, V>();
  }

  public static <T> LinkedHashSet<T> newLinkedHashSet(T... elements) {
    return new LinkedHashSet<T>(Arrays.asList(elements));
  }

  public interface Function<F, T> {
    T apply(F from);
  }

  public interface Supplier<T> {
    T get();
  }
}
