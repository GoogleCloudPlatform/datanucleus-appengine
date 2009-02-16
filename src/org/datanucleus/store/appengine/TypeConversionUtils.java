// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Blob;
import com.google.common.collect.PrimitiveArrays;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.ContainerMetaData;
import org.datanucleus.sco.SCOUtils;
import org.datanucleus.store.appengine.Utils.Function;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Utility methods for converting between datastore types and pojo types.
 *
 * @author Max Ross <maxr@google.com>
 */
class TypeConversionUtils {
  TypeConversionUtils() {}

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
      buildDatastoreToPojoTypeFuncMap();

  private static Map<Class<?>, Function<Object, Object>> buildDatastoreToPojoTypeFuncMap() {
    Map<Class<?>, Function<Object, Object>> map = new HashMap<Class<?>, Function<Object, Object>>();
    map.put(Integer.class, LONG_TO_INTEGER);
    map.put(Integer.TYPE, LONG_TO_INTEGER);
    map.put(Short.class, LONG_TO_SHORT);
    map.put(Short.TYPE, LONG_TO_SHORT);
    map.put(Byte.class, LONG_TO_BYTE);
    map.put(Byte.TYPE, LONG_TO_BYTE);
    map.put(Character.class, LONG_TO_CHARACTER);
    map.put(Character.TYPE, LONG_TO_CHARACTER);
    map.put(Float.class, DOUBLE_TO_FLOAT);
    map.put(Float.TYPE, DOUBLE_TO_FLOAT);
    return map;
  }

  /**
   * @param metaData The meta data we'll consult.
   *
   * @return {@code true} if the pojo property is an array of bytes.
   */
  public boolean pojoPropertyIsByteArray(AbstractMemberMetaData metaData) {
    ContainerMetaData cmd = metaData.getContainer();
    if (cmd instanceof ArrayMetaData) {
      String containerType = ((ArrayMetaData)cmd).getElementType();
      return containerType.equals(Byte.class.getName()) ||
          containerType.equals(Byte.TYPE.getName());
    }
    return false;
  }

  /**
   * @param metaData The meta data we'll consult.
   *
   * @return {@code true} if the pojo property is a Collection of Characters.
   */
  public boolean pojoPropertyIsCharacterCollection(AbstractMemberMetaData metaData) {
    ContainerMetaData cmd = metaData.getContainer();
    if (cmd instanceof CollectionMetaData) {
      String containerType = ((CollectionMetaData)cmd).getElementType();
      return containerType.equals(Character.class.getName());
    }
    return false;
  }

  /**
   * Converts a pojo array of bytes to a datastore blob.
   *
   * @param value The pojo array.  It could be a primitive array, which is why
   * the type of the param is Object and not Object[].
   *
   * @return A Blob representing the byte array.
   */
  public Blob convertByteArrayToBlob(Object value) {
    if (value.getClass().getComponentType().isPrimitive()) {
      return new Blob((byte[]) value);
    } else {
      Byte[] bytes = (Byte[]) value;
      byte[] array = (byte[]) Array.newInstance(Byte.TYPE, bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        array[i] = bytes[i];
      }
      return new Blob(array);
    }
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
  public List<?> convertPojoArrayToDatastoreList(Object value) {
    // special case logic for Character, which is not supported by the datastore
    if (value.getClass().getComponentType().isPrimitive()) {
      if (value.getClass().getComponentType().equals(Character.TYPE)) {
        return Utils.transform(PrimitiveArrays.asList((char[]) value), CHARACTER_TO_LONG);
      }
      // Primitive arrays do not extend Object[] so they need
      // special handling.
      return PrimitiveUtils.PRIMITIVE_ARRAY_TO_LIST_FUNC_MAP
          .get(value.getClass().getComponentType()).apply(value);
    }
    return convertNonPrimitivePojoArrayToDatastoreList((Object[]) value);
  }

  private List<?> convertNonPrimitivePojoArrayToDatastoreList(Object[] array) {
    // special case logic for Character, which is not supported by the datastore
    if (array.getClass().getComponentType().equals(Character.class)) {
      return Utils.transform(Arrays.asList((Character[]) array), CHARACTER_TO_LONG);
    }
    return Arrays.asList(array);
  }

  /**
   * Performs type conversions on a datastore property value.  Note that
   * if the property is an array and the value is {@code null}, this method
   * will return a zero-length array of the appropriate type.  Similarly, if
   * the property is a {@link Collection} and the value is {@code null}, this
   * method will return an empty {@link Collection} of the appropriate type.
   *
   * @param clr class loader resolver to use for string to class
   * conversions
   * @param value The datastore value.
   * @param ownerSM The owning state manager.  Used for creating change-detecting
   * wrappers.
   * @param ammd The meta data for the pojo property which will eventually
   * receive the result of the conversion.
   *
   * @return A representation of the datastore property value that can be set
   * on the pojo.
   */
  public Object datastoreValueToPojoValue(
      ClassLoaderResolver clr, Object value, StateManager ownerSM, AbstractMemberMetaData ammd) {
    ContainerMetaData cmd = ammd.getContainer();
    if (pojoPropertyIsArray(ammd)) {
      String memberTypeStr = ((ArrayMetaData)cmd).getElementType();
      Class<?> memberType = classForName(clr, memberTypeStr);

      if (value instanceof Blob) {
        if (memberType != Byte.TYPE && memberType != Byte.class) {
          throw new NucleusException("Cannot convert a Blob to an array of type " + memberTypeStr);
        }
        value = convertDatastoreBlobToByteArray((Blob) value, memberType);
      } else {
        // The pojo property is an array.  The datastore only supports
        // Collections so we need to translate.
        @SuppressWarnings("unchecked")
        List<Object> datastoreList = (List<Object>) value;
        value = convertDatastoreListToPojoArray(datastoreList, memberType);
      }
    } else if (pojoPropertyIsCollection(cmd)) {
      CollectionMetaData collMetaData = (CollectionMetaData) cmd;
      String pojoTypeStr = collMetaData.getElementType();
      Class<?> pojoType = classForName(clr, pojoTypeStr);
      @SuppressWarnings("unchecked")
      List<Object> datastoreList = (List<Object>) value;
      if (pojoPropertyIsList(ammd)) {
        @SuppressWarnings("unchecked")
        Class<? extends List> listType = ammd.getType();
        value = convertDatastoreListToPojoList(datastoreList, pojoType, listType);
      } else if (pojoPropertyIsSet(ammd)) {
        @SuppressWarnings("unchecked")
        Class<? extends Set> setType = ammd.getType();
        value = convertDatastoreListToPojoSet(datastoreList, pojoType, setType);
      }
      value = wrap(ownerSM, ammd, value);
    } else { // neither array nor collection
      if (value != null) {
        // nothing to convert
        value = getDatastoreTypeToPojoTypeFunc(Utils.identity(), ammd).apply(value);
      }
    }
    return value;
  }

  Object wrap(StateManager ownerSM, AbstractMemberMetaData ammd, Object value) {
    // Wrap the provided value in a state-manager aware object.  This allows
    // us to detect changes to Lists, Sets, etc.
    return SCOUtils.newSCOInstance(
        ownerSM, ammd, ammd.getType(), value.getClass(), value, true, true, true);
  }

  /**
   * Converts a datastore blob to either a byte[] or a Byte[].
   *
   * @param blob The blob to convert.
   * @param pojoType The destination type for the array.
   * @return Object instead of Object[] because primitive arrays don't extend
   * Object[].
   */
  private Object convertDatastoreBlobToByteArray(Blob blob, Class<?> pojoType) {
    if (pojoType.isPrimitive()) {
      return blob.getBytes();
    } else {
      byte[] bytes = blob.getBytes();
      Byte[] array = (Byte[]) Array.newInstance(pojoType, bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        array[i] = bytes[i];
      }
      return array;
    }
  }

  List<Object> convertDatastoreListToPojoList(List<Object> datastoreList, Class<?> pojoType,
      Class<? extends List> listType) {
    List<Object> listToReturn;
    if (listType.isInterface()) {
      listToReturn = Utils.newArrayList();
    } else {
      try {
        listToReturn = listType.newInstance();
      } catch (InstantiationException e) {
        throw new NucleusException("Cannot instantiate List of type " + listType.getName(), e);
      } catch (IllegalAccessException e) {
        throw new NucleusException("Cannot instantiate List of type " + listType.getName(), e);
      }
    }
    if (datastoreList == null) {
      return listToReturn;
    }
    Function<Object, Object> func = DATASTORE_TYPE_TO_POJO_TYPE_FUNC.get(pojoType);
    if (func != null) {
      datastoreList = Utils.transform(datastoreList, func);
    } else if (Enum.class.isAssignableFrom(pojoType)) {
      @SuppressWarnings("unchecked")
      Class<Enum> enumClass = (Class<Enum>) pojoType;
      datastoreList = new ArrayList<Object>(
          Arrays.asList(convertStringListToEnumArray(datastoreList, enumClass)));
    }
    if (listType.isAssignableFrom(datastoreList.getClass())) {
      return datastoreList;
    }
    listToReturn.addAll(datastoreList);
    return listToReturn;
  }

  Set<Object> convertDatastoreListToPojoSet(List<Object> datastoreList, Class<?> pojoType,
      Class<? extends Set> setType) {
    List<?> convertedList =
        convertDatastoreListToPojoList(datastoreList, pojoType, ArrayList.class);
    Set<Object> setToReturn;
    if (Set.class.equals(setType)) {
      setToReturn = Utils.newHashSet();
    } else if (SortedSet.class.equals(setType)) {
      setToReturn = Utils.newTreeSet();
    } else {
      try {
        setToReturn = setType.newInstance();
      } catch (InstantiationException e) {
        throw new NucleusException("Cannot instantiate Set of type " + setType.getName(), e);
      } catch (IllegalAccessException e) {
        throw new NucleusException("Cannot instantiate Set of type " + setType.getName(), e);
      }
    }
    setToReturn.addAll(convertedList);
    return setToReturn;
  }

  /**
   * Returns Object instead of Object[] because primitive arrays don't extend
   * Object[].
   */
  Object convertDatastoreListToPojoArray(List<Object> datastoreList, Class<?> pojoType) {
    datastoreList = convertDatastoreListToPojoList(datastoreList, pojoType, ArrayList.class);
    // We need to see whether we're converting to a primitive array or an
    // array that extends Object[].
    if (pojoType.isPrimitive()) {
      return PrimitiveUtils.LIST_TO_PRIMITIVE_ARRAY_FUNC_MAP.get(pojoType).apply(datastoreList);
    }
    Object[] array = (Object[]) Array.newInstance(pojoType, datastoreList.size());
    return datastoreList.toArray(array);
  }

  private Object[] convertStringListToEnumArray(List<?> datastoreList, Class<Enum> pojoType) {
    Object[] result = (Object[]) Array.newInstance(pojoType, datastoreList.size());
    int i = 0;
    for (Object obj : datastoreList) {
      result[i++] = obj == null ? null : Enum.valueOf(pojoType, (String) obj);
    }
    return result;
  }

  private Class<?> classForName(ClassLoaderResolver clr, String typeStr) {
    // If typeStr is a primitive it is not a class we can look up using
    // Class.forName.  Consult our map of primitive classnames to see
    // if this is the case.
    Class<?> clazz = PrimitiveUtils.PRIMITIVE_CLASSNAMES.get(typeStr);
    if (clazz == null) {
      clazz = clr.classForName(typeStr);
    }
    return clazz;
  }

  /**
   * Get the conversion function for the field identified by the given field
   * number, returning the provided default if no conversion function exists.
   */
  private Function<Object, Object> getDatastoreTypeToPojoTypeFunc(
      Function<Object, Object> defaultVal, AbstractMemberMetaData ammd) {
    Function<Object, Object> candidate = DATASTORE_TYPE_TO_POJO_TYPE_FUNC.get(ammd.getType());
    return candidate != null ? candidate : defaultVal;
  }

  private boolean pojoPropertyIsCollection(ContainerMetaData cmd) {
    return cmd instanceof CollectionMetaData;
  }

  private boolean pojoPropertyIsList(AbstractMemberMetaData ammd) {
    return List.class.isAssignableFrom(ammd.getType());
  }

  private boolean pojoPropertyIsSet(AbstractMemberMetaData ammd) {
    return Set.class.isAssignableFrom(ammd.getType());
  }

  private boolean pojoPropertyIsArray(AbstractMemberMetaData ammd) {
    return ammd.getContainer() instanceof ArrayMetaData;
  }

  public static final Function<Character, Long> CHARACTER_TO_LONG =
      new Function<Character, Long>() {
    public Long apply(Character character) {
      return Long.valueOf(character);
    }
  };

  public List<String> convertEnumsToStringList(Iterable<Enum> enums) {
    List<String> result = Utils.newArrayList();
    for (Enum e : enums) {
      result.add(e == null ? null : e.name());
    }
    return result;
  }
}
