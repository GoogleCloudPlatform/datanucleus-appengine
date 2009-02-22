// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

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

  public static <F, T> List<T> transform(Collection<F> from, Function<? super F, ? extends T> func) {
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
}
