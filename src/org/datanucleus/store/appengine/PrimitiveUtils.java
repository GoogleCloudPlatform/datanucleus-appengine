// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMapBuilder;
import com.google.common.collect.PrimitiveArrays;

import java.util.Map;
import java.util.List;

/**
 * Utility methods for dealing with primitive types.
 *
 * @author Max Ross <maxr@google.com>
 */
final class PrimitiveUtils {

  private PrimitiveUtils() {}

  /**
   * Maps the result of <Primitive>.TYPE.getName() to value.
   * This is to get around the fact that you can't look up
   * primitive type classes using Class.forName()
   */
  public static final Map<String, Class<?>> PRIMITIVE_CLASSNAMES =
      new ImmutableMapBuilder<String, Class<?>>()
      .put(Boolean.TYPE.getName(), Boolean.TYPE)
      .put(Integer.TYPE.getName(), Integer.TYPE)
      .put(Long.TYPE.getName(), Long.TYPE)
      .put(Short.TYPE.getName(), Short.TYPE)
      .put(Character.TYPE.getName(), Character.TYPE)
      .put(Byte.TYPE.getName(), Byte.TYPE)
      .put(Double.TYPE.getName(), Double.TYPE)
      .put(Float.TYPE.getName(), Float.TYPE).getMap();

  /**
   * Gives us a fast way to get from a primitive type to a function that
   * transforms a primitive array of that type to a List of that type.
   * We intentionally omit Character since the datastore does not support it.
   */
  public static final
  Map<Class<?>, Function<Object, List<?>>> PRIMITIVE_ARRAY_TO_LIST_FUNC_MAP =
      new ImmutableMapBuilder<Class<?>, Function<Object, List<?>>>()
    .put(
        Integer.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((int[]) o);
          }
        })
    .put(
        Long.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((long[]) o);
          }
        })
    .put(
        Short.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((short[]) o);
          }
        })
    .put(
        Byte.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((byte[]) o);
          }
        })
    .put(
        Float.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((float[]) o);
          }
        })
    .put(
        Double.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((double[]) o);
          }
        })
    .put(
        Boolean.TYPE,
        new Function<Object, List<?>>() {
          public List<?> apply(Object o) {
            return PrimitiveArrays.asList((boolean[]) o);
          }
        }).getMap();


  /**
   * Gives us a fast way to get from a primitive type to a function that
   * transforms a List of that type to a primitive array of that type.
   */
  public static final
  Map<Class<?>, Function<List<?>, Object>> LIST_TO_PRIMITIVE_ARRAY_FUNC_MAP =
      new ImmutableMapBuilder<Class<?>, Function<List<?>, Object>>()
    .put(
        Integer.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toIntArray((List<Integer>) list);
          }
        })
    .put(
        Short.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toShortArray((List<Short>) list);
          }
        })
    .put(
        Long.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toLongArray((List<Long>) list);
          }
        })
    .put(
        Character.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toCharArray((List<Character>) list);
          }
        })
    .put(
        Float.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toFloatArray((List<Float>) list);
          }
    })
    .put(
        Double.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toDoubleArray((List<Double>) list);
          }
        })
    .put(
        Boolean.TYPE, new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toBooleanArray((List<Boolean>) list);
          }
        })
    .put(
        Byte.TYPE,
        new Function<List<?>, Object>() {
          @SuppressWarnings("unchecked")
          public Object apply(List<?> list) {
            return PrimitiveArrays.toByteArray((List<Byte>) list);
          }
      }).getMap();

}
