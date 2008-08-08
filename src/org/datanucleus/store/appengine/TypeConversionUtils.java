// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMapBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.PrimitiveArrays;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.lang.reflect.Array;

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ContainerMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.exceptions.NucleusException;

/**
 * Utility methods for converting between datastore types and pojo types.
 *
 * @author Max Ross <maxr@google.com>
 */
final class TypeConversionUtils {
  private TypeConversionUtils() {}

  /**
   * A {@link Function} that converts {@link Long} to {@link Integer}.
   */
  private static final Function<Object, Object> LONG_TO_INTEGER = new Function<Object, Object>() {
    public Integer apply(Object in) {
      return ((Long) in).intValue();
    }
  };

  /**
   * A {@link Function} that converts {@link Long} to {@link Short}.
   */
  private static final Function<Object, Object> LONG_TO_SHORT = new Function<Object, Object>() {
    public Short apply(Object in) {
      return ((Long) in).shortValue();
    }
  };

  /**
   * A {@link Function} that converts {@link Long} to {@link Byte}.
   */
  private static final Function<Object, Object> LONG_TO_BYTE = new Function<Object, Object>() {
    public Byte apply(Object in) {
      return ((Long) in).byteValue();
    }
  };

  /**
   * A {@link Function} that converts {@link Long} to {@link Character}.
   */
  private static final Function<Object, Object> LONG_TO_CHARACTER = new Function<Object, Object>() {
    public Character apply(Object in) {
      return (char) (long) (Long) in;
    }
  };

  /**
   * A {@link Function} that converts {@link Double} to {@link Float}.
   */
  private static final Function<Object, Object> DOUBLE_TO_FLOAT = new Function<Object, Object>() {
    public Float apply(Object in) {
      return ((Double) in).floatValue();
    }
  };

  /**
   * Maps primitive types to functions which demote from the datastore
   * representation to the primitive type.  Note that we map both
   * the true primitive class and the object version.
   */
  private static final Map<Class<?>, Function<Object, Object>> DATASTORE_TYPE_TO_POJO_TYPE_FUNC =
      new ImmutableMapBuilder<Class<?>, Function<Object, Object>>()
      .put(Integer.class, LONG_TO_INTEGER)
      .put(Integer.TYPE, LONG_TO_INTEGER)
      .put(Short.class, LONG_TO_SHORT)
      .put(Short.TYPE, LONG_TO_SHORT)
      .put(Byte.class, LONG_TO_BYTE)
      .put(Byte.TYPE, LONG_TO_BYTE)
      .put(Character.class, LONG_TO_CHARACTER)
      .put(Character.TYPE, LONG_TO_CHARACTER)
      .put(Float.class, DOUBLE_TO_FLOAT)
      .put(Float.TYPE, DOUBLE_TO_FLOAT)
      .getMap();


  /**
   * @param metaData The meta data we'll consult.
   *
   * @return {@code true} if the pojo property is a Collection of Characters.
   */
  public static boolean pojoPropertyIsCharacterCollection(AbstractMemberMetaData metaData) {
    ContainerMetaData cmd = metaData.getContainer();
    if (cmd instanceof CollectionMetaData) {
      String containerType = ((CollectionMetaData)cmd).getElementType();
      return containerType.equals(Character.class.getName());
    }
    return false;
  }

  /**
   * Converts a pojo array to a datastore list, transforming from pojo types
   * to datastore types along the way.
   *
   * @param value The pojo array.  It could be a primitive array, which is why
   * the type of the param is Object and not Object[].
   *
   * @return A List containing the values in the array that was provided.
   */
  public static List<?> convertPojoArrayToDatastoreList(Object value) {
    // special case logic for Character, which is not supported by the datastore
    if (value.getClass().getComponentType().isPrimitive()) {
      if (value.getClass().getComponentType().equals(Character.TYPE)) {
        return Lists.transform(PrimitiveArrays.asList((char[]) value), CHARACTER_TO_LONG);
      }
      // Primitive arrays do not extend Object[] so they need
      // special handling.
      return PrimitiveUtils.PRIMITIVE_ARRAY_TO_LIST_FUNC_MAP
          .get(value.getClass().getComponentType()).apply(value);
    }
    return convertNonPrimitivePojoArrayToDatastoreList((Object[]) value);
  }

  private static List<?> convertNonPrimitivePojoArrayToDatastoreList(Object[] array) {
    // special case logic for Character, which is not supported by the datastore
    if (array.getClass().getComponentType().equals(Character.class)) {
      return Lists.transform(Arrays.asList((Character[]) array), CHARACTER_TO_LONG);
    }
    return Arrays.asList(array);
  }

  /**
   * Performs type conversions on a datastore property value.
   *
   * @param value The datastore value.
   * @param ammd The meta data for the pojo property which will eventually
   * receive the result of the conversion.
   *
   * @return A representation of the datastore property value that can be set
   * on the pojo.
   */
  public static Object datastoreValueToPojoValue(Object value, AbstractMemberMetaData ammd) {
    if (value == null) {
      // nothing to convert
      return value;
    }
    ContainerMetaData cmd = ammd.getContainer();
    if (pojoPropertyIsArray(ammd)) {
      // The pojo property is an array.  The datastore only supports
      // Collections so we need to translate.
      String pojoTypeStr = ((ArrayMetaData)cmd).getElementType();
      Class<?> pojoType = classForName(pojoTypeStr);
      value = convertDatastoreListToPojoArray((List<?>) value, pojoType);
    } else if (pojoPropertyIsCollection(ammd)) {
      String pojoTypeStr = ((CollectionMetaData)cmd).getElementType();
      Class<?> pojoType = classForName(pojoTypeStr);
      value = convertDatastoreListToPojoCollection((List<?>) value, pojoType);
    } else {
      value = getDatastoreTypeToPojoTypeFunc(Functions.identity(), ammd).apply(value);
    }
    return value;
  }

  private static List<?> convertDatastoreListToPojoCollection(
      List<?> datastoreList, Class<?> pojoType) {
    Function<Object, Object> func = DATASTORE_TYPE_TO_POJO_TYPE_FUNC.get(pojoType);
    if (func != null) {
      datastoreList = Lists.transform(datastoreList, func);
    }
    return datastoreList;
  }

  /**
   * Returns Object instead of Object[] because primitive arrays don't extend
   * Object[].
   */
  private static Object convertDatastoreListToPojoArray(List<?> datastoreList, Class<?> pojoType) {
    datastoreList = convertDatastoreListToPojoCollection(datastoreList, pojoType);
    // We need to see whether we're converting to a primitive array or an
    // array that extends Object[].
    if (pojoType.isPrimitive()) {
      return PrimitiveUtils.LIST_TO_PRIMITIVE_ARRAY_FUNC_MAP.get(pojoType).apply(datastoreList);
    }
    Object[] array = (Object[]) Array.newInstance(pojoType, datastoreList.size());
    return datastoreList.toArray(array);
  }

  private static Class<?> classForName(String typeStr) {
    // If typeStr is a primitive it is not a class we can look up using
    // Class.forName.  Consult our map of primitive classnames to see
    // if this is the case.
    Class<?> clazz = PrimitiveUtils.PRIMITIVE_CLASSNAMES.get(typeStr);
    if (clazz == null) {
      try {
        clazz = Class.forName(typeStr);
      } catch (ClassNotFoundException e) {
        throw new NucleusException("Could not locate container element type " + typeStr, e);
      }
    }
    return clazz;
  }

  /**
   * Get the conversion function for the field identified by the given field
   * number, returning the provided default if no conversion function exists.
   */
  private static Function<Object, Object> getDatastoreTypeToPojoTypeFunc(
      Function<Object, Object> defaultVal, AbstractMemberMetaData ammd) {
    Function<Object, Object> candidate = DATASTORE_TYPE_TO_POJO_TYPE_FUNC.get(ammd.getType());
    return candidate != null ? candidate : defaultVal;
  }

  private static boolean pojoPropertyIsCollection(AbstractMemberMetaData ammd) {
    return ammd.getContainer() instanceof CollectionMetaData;
  }

  private static boolean pojoPropertyIsArray(AbstractMemberMetaData ammd) {
    return ammd.getContainer() instanceof ArrayMetaData;
  }

  public static final Function<Character, Long> CHARACTER_TO_LONG =
      new Function<Character, Long>() {
    public Long apply(Character character) {
      return Long.valueOf(character);
    }
  };
}
