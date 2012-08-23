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

import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.sco.SCOUtils;

import com.google.appengine.datanucleus.Utils.Function;

import javax.persistence.Entity;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;

/**
 * Utility methods for converting between datastore types and pojo types.
 *
 * @author Max Ross <maxr@google.com>
 */
public class TypeConversionUtils {

  private final boolean encodeBigDecimalAsString;
  
  /**
   * Maps primitive types to functions which promote from the primitive type to
   * the datastore representation. Note that we map both the true primitive
   * class and the object version.
   */
  private final Map<Class<?>, Function<Object, Object>> pojoToDatastoreTypeFunction;

  private final Set<Class> supportedClasses;

  private static final Set<Class> buildSupportedClasses() {
    Set<Class> classes = new HashSet<Class>();
    classes.add(Byte.class);
    classes.add(Boolean.class);
    classes.add(Character.class);
    classes.add(Double.class);
    classes.add(Float.class);
    classes.add(Integer.class);
    classes.add(Long.class);
    classes.add(Short.class);
    classes.add(byte.class);
    classes.add(boolean.class);
    classes.add(char.class);
    classes.add(double.class);
    classes.add(float.class);
    classes.add(int.class);
    classes.add(long.class);
    classes.add(short.class);
    classes.add(String.class);
    classes.add(Enum.class);
    classes.add(Date.class);
    classes.add(BigDecimal.class);
    return classes;
  }

  /**
   * A {@link Function} that converts {@link Long} to {@link Integer}.
   */
  private static final Function<Object, Object> LONG_TO_INTEGER = new Function<Object, Object>() {
    public Integer apply(Object in) {
      return null==in ? null : ((Long) in).intValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Long} to {@link Short}.
   */
  private static final Function<Object, Object> LONG_TO_SHORT = new Function<Object, Object>() {
    public Short apply(Object in) {
      return null==in ? null : ((Long) in).shortValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Long} to {@link Byte}.
   */
  private static final Function<Object, Object> LONG_TO_BYTE = new Function<Object, Object>() {
    public Byte apply(Object in) {
      return null==in ? null : ((Long) in).byteValue() ;
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
      return null==in ? null : ((Double) in).floatValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Double} or {@link String} to {@link BigDecimal}.
   */
  private static final Function<Object, Object> STRING_OR_DOUBLE_TO_BIG_DECIMAL = new Function<Object, Object>() {
    public BigDecimal apply(Object in) {
      if (in == null) {
        return null;
      }
      if (in instanceof String) {
        return BigDecimals.fromSortableString((String) in);
      } else {
        return new BigDecimal((Double) in);
      }
    }
  };

  /**
   * A {@link Function} that converts {@link String} to {@link Text}.
   */
  private static final Function<Object, Object> STRING_TO_TEXT = new Function<Object, Object>() {
    public Text apply(Object in) {
      if (in instanceof String) {
        return null==in ? null : new Text((String) in);
      }
      return (Text)in;
    }
  };

  /**
   * A {@link Function} that converts {@link Integer} to {@link Long}.
   */
  private final Function<Object, Object> integerToLong = new Function<Object, Object>() {
    public Long apply(Object in) {
      if (in instanceof Long) {
        return (Long)in;
      }
      return null==in ? null : ((Integer) in).longValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Short} to {@link Long}.
   */
  private final Function<Object, Object> shortToLong = new Function<Object, Object>() {
    public Long apply(Object in) {
      if (in instanceof Long) {
        return (Long)in;
      }
      return null==in ? null : ((Short) in).longValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Character} to {@link Long}.
   */
  private final Function<Object, Object> characterToLong = new Function<Object, Object>() {
    public Long apply(Object character) {
      return null==character ? null : Long.valueOf((Character) character);
    }
  };

  /**
   * A {@link Function} that converts {@link Byte} to {@link Long}.
   */
  private final Function<Object, Object> byteToLong = new Function<Object, Object>() {
    public Long apply(Object in) {
      if (in instanceof Long) {
        return (Long)in;
      }
      return null==in ? null : ((Byte) in).longValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link Float} to {@link Double}.
   */
  private final Function<Object, Object> floatToDouble = new Function<Object, Object>() {
    public Double apply(Object in) {
      if (in instanceof Double) {
        return (Double)in;
      }
      return null==in ? null : ((Float) in).doubleValue() ;
    }
  };

  /**
   * A {@link Function} that converts {@link BigDecimal} to {@link String}.
   */
  private final Function<Object, Object> bigDecimalToDoubleOrString = new Function<Object, Object>() {
    public Object apply(Object in) {
      if (encodeBigDecimalAsString) {
        return null==in ? null : BigDecimals.toSortableString((BigDecimal) in);
      } else {
        return null==in ? null : ((BigDecimal) in).doubleValue();
      }
    }
  };

  /**
   * Maps primitive types to functions which demote from the datastore
   * representation to the primitive type.  Note that we map both
   * the true primitive class and the object version.
   */
  private static final Map<Class<?>, Function<Object, Object>> DATASTORE_TO_POJO_TYPE_FUNC =
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
    map.put(BigDecimal.class, STRING_OR_DOUBLE_TO_BIG_DECIMAL);
    map.put(Text.class, STRING_TO_TEXT);
    return map;
  }

  private Map<Class<?>, Function<Object, Object>> buildPojoToDatastoreTypeFuncMap() {
    Map<Class<?>, Function<Object, Object>> map = new HashMap<Class<?>, Function<Object, Object>>();
    map.put(Integer.class, integerToLong);
    map.put(Integer.TYPE, integerToLong);
    map.put(Short.class, shortToLong);
    map.put(Short.TYPE, shortToLong);
    map.put(Byte.class, byteToLong);
    map.put(Byte.TYPE, byteToLong);
    map.put(Character.class, characterToLong);
    map.put(Character.TYPE, characterToLong);
    map.put(Float.class, floatToDouble);
    map.put(Float.TYPE, floatToDouble);
    map.put(BigDecimal.class, bigDecimalToDoubleOrString);
    return map;
  }

  private static Function<Object, Object> IDENTITY = new Function<Object, Object>() {
    public Object apply(Object from) {
      return from;
    }
  };

  TypeConversionUtils(boolean encodeBigDecimalAsString) {
    this.encodeBigDecimalAsString = encodeBigDecimalAsString;
    this.supportedClasses = buildSupportedClasses();
    this.pojoToDatastoreTypeFunction = buildPojoToDatastoreTypeFuncMap();
  }
  
  /**
   * @param metaData The meta data we'll consult.
   *
   * @return {@code true} if the pojo property is an array of bytes.
   */
  private boolean pojoPropertyIsByteArray(AbstractMemberMetaData metaData) {
    Class<?> componentType = metaData.getType().getComponentType();
    return componentType.equals(Byte.class) || componentType.equals(Byte.TYPE);
  }

  /**
   * @param metaData The meta data we'll consult.
   *
   * @return {@code true} if the pojo property is a collection of bytes.
   */
  private boolean pojoPropertyIsByteCollection(AbstractMemberMetaData metaData) {
    String containerClassStr = metaData.getCollection().getElementType();
    return containerClassStr.equals(Byte.class.getName()) ||
           containerClassStr.equals(Byte.TYPE.getName());
  }

  /**
   * Converts an array of bytes to a datastore short blob.
   *
   * @param value The pojo array.  It could be a primitive array, which is why
   * the type of the param is Object and not Object[].
   *
   * @return A {@code ShortBlob} representing the byte array.
   */
  private ShortBlob convertByteArrayToShortBlob(Object value) {
    if (value.getClass().getComponentType().isPrimitive()) {
      return new ShortBlob((byte[]) value);
    } else {
      return convertByteCollectionToShortBlob(Arrays.asList((Byte[]) value));
    }
  }

  /**
   * Converts a collection of bytes to a datastore short blob.
   *
   * @param value The pojo collection.
   *
   * @return A {@code ShortBlob} representing the byte collection.
   */
  private ShortBlob convertByteCollectionToShortBlob(Collection<Byte> value) {
    return new ShortBlob(PrimitiveArrays.toByteArray(value));
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
  private List<?> convertPojoArrayToDatastoreList(Object value) {
    Class<?> componentType = value.getClass().getComponentType();
    if (componentType.isPrimitive()) {
      // Primitive arrays do not extend Object[] so they need
      // special handling.
      List<Object> datastoreList =
          (List<Object>) PrimitiveUtils.PRIMITIVE_ARRAY_TO_LIST_FUNC_MAP.get(componentType).apply(value);
      if (pojoToDatastoreTypeFunction.get(componentType) != null) {
        datastoreList =
            Utils.transform(datastoreList, pojoToDatastoreTypeFunction.get(componentType));
      }
      return datastoreList;
    }
    // cast to Object[] is safe because we know it's not an array of primitives
    return convertNonPrimitivePojoArrayToDatastoreList((Object[]) value);
  }

  private List<?> convertNonPrimitivePojoArrayToDatastoreList(Object[] array) {
    List<?> datastoreList = Arrays.asList(array);
    if (pojoToDatastoreTypeFunction.get(array.getClass().getComponentType()) != null) {
      datastoreList = Utils.transform(Arrays.asList(array),
          pojoToDatastoreTypeFunction.get(array.getClass().getComponentType()));
    }
    return datastoreList;
  }

  /**
   * Performs type conversions on a datastore property value.  Note that
   * if the property is an array and the value is {@code null}, this method
   * will return a zero-length array of the appropriate type.  Similarly, if
   * the property is a {@link Collection} and the value is {@code null}, this
   * method will return an empty {@link Collection} of the appropriate type.
   * @param typeMgr TypeManager
   * @param clr class loader resolver to use for string to class conversions
   * @param value The datastore value. Can be null.
   * @param ammd The meta data for the pojo member that this value applies to
   *
   * @return A representation of the datastore property value that can be set on the pojo.
   */
  public Object datastoreValueToPojoValue(TypeManager typeMgr, ClassLoaderResolver clr, Object value, 
      AbstractMemberMetaData ammd) {
    if (ammd.hasArray()) {
      value = datastoreValueToPojoArray(value, ammd);
    } else if (ammd.hasCollection()) {
      value = datastoreValueToPojoCollection(clr, value, ammd);
    } else if (ammd.hasMap()) {
      value = datastoreValueToPojoMap(clr, value, ammd);
    } else {
      if (value != null) {
        if (java.sql.Time.class.isAssignableFrom(ammd.getType())) {
          // Long -> java.sql.Time
          value = new java.sql.Time((Long)value);
        } else if (java.sql.Date.class.isAssignableFrom(ammd.getType())) {
          // Long -> java.sql.Date
          value = new java.sql.Date((Long)value);
        } else if (java.sql.Timestamp.class.isAssignableFrom(ammd.getType())) {
          // Long -> java.sql.Timestamp
          value = new java.sql.Timestamp((Long)value);
        } else if (Enum.class.isAssignableFrom(ammd.getType())) {
          // String(name) / Long(ordinal) -> Enum
          boolean asOrdinal = false;
          if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
            String jdbcType = ammd.getColumnMetaData()[0].getJdbcType();
            if (jdbcType != null && jdbcType.equalsIgnoreCase("integer")) {
              // Persisted as ordinal
              asOrdinal = true;
            }
          }
          Class<Enum> enumClass = ammd.getType();
          if (asOrdinal) {
            value = enumClass.getEnumConstants()[(Integer)value];
          } else {
            value = Enum.valueOf(enumClass, (String) value);
          }
        } else {
          if (supportedClasses.contains(ammd.getType()) ||
              ammd.getTypeName().startsWith("com.google.appengine.api")) {
            value = getDatastoreToPojoTypeFunc(Utils.identity(), ammd.getType()).apply(value);
          } else if (value instanceof String) {
            TypeConverter conv = typeMgr.getTypeConverterForType(ammd.getType(), String.class);
            if (conv != null) {
              // Persisted as String, so convert back
              value = conv.toMemberType((String)value);
            }
          } else if (value instanceof Long) {
            TypeConverter conv = typeMgr.getTypeConverterForType(ammd.getType(), Long.class);
            if (conv != null) {
              // Persisted as Long, so convert back
              value = conv.toMemberType((Long)value);
            }
          } else {
            // Unsupported type on GAE/J ?
            value = getDatastoreToPojoTypeFunc(Utils.identity(), ammd.getType()).apply(value);
          }
        }
      }
    }
    return value;
  }

  /**
   *
   * @param clr
   * @param value Can be null.
   * @param ammd
   * @param cmd
   * @return
   */
  private Object datastoreValueToPojoCollection(ClassLoaderResolver clr, Object value, AbstractMemberMetaData ammd) {
    String memberTypeStr = ammd.getCollection().getElementType();
    Class<?> memberType = classForName(clr, memberTypeStr);
    Function<Object, Object> conversionFunc = DATASTORE_TO_POJO_TYPE_FUNC.get(memberType);
    if (value instanceof ShortBlob) {
      if (!memberType.equals(Byte.TYPE) && !memberType.equals(Byte.class)) {
        throw new NucleusException(
            "Cannot convert a ShortBlob to an array of type " + memberType.getName());
      }
      value = Arrays.asList((Byte[]) convertShortBlobToByteArray((ShortBlob) value, memberType));
      // We don't want any conversions applied - we've got bytes from the
      // datastore and the property on the pojo wants bytes.
      conversionFunc = null;
    }
    @SuppressWarnings("unchecked")
    List<Object> datastoreList = (List<Object>) value;
    if (pojoPropertyIsSet(ammd)) {
      @SuppressWarnings("unchecked")
      Class<? extends Set> setType = ammd.getType();
      value = convertDatastoreListToPojoSet(datastoreList, memberType, setType, conversionFunc);
    } else if (pojoPropertyIsCollection(ammd)) {
      @SuppressWarnings("unchecked")
      Class<? extends Collection> listType = ammd.getType();
      value = convertDatastoreListToPojoCollection(datastoreList, memberType, listType, conversionFunc);
    }
    return value;
  }

  /**
   *
   * @param value The value we're converting to an array.  Can be null
   * @param ammd The meta data for the field
   * @return The datastore value converted to an array (potentially a primitive
   * array, which is why we return Object and not Object[]).  Can be null.
   */
  private Object datastoreValueToPojoArray(Object value, AbstractMemberMetaData ammd) {
    Class<?> memberType = ammd.getType().getComponentType();
    if (value instanceof ShortBlob) {
      if (!memberType.equals(Byte.TYPE) && !memberType.equals(Byte.class)) {
        throw new NucleusException(
            "Cannot convert a ShortBlob to an array of type " + memberType.getName());
      }
      value = convertShortBlobToByteArray((ShortBlob) value, memberType);
    } else {
      // The pojo property is an array.  The datastore only supports
      // Collections so we need to translate.
      @SuppressWarnings("unchecked")
      List<Object> datastoreList = (List<Object>) value;
      value = convertDatastoreListToPojoArray(datastoreList, memberType);
    }
    return value;
  }

  /**
   * Convert a datastore value back into a Map.
   * @param value The value we're converting to a map. Can be null
   * @param ammd The meta data for the field
   * @return The datastore value converted to an array (potentially a primitive
   * array, which is why we return Object and not Object[]).  Can be null.
   */
  private Object datastoreValueToPojoMap(ClassLoaderResolver clr, Object value, AbstractMemberMetaData ammd) {
    if (value == null || !(value instanceof List)) {
      return value;
    }

    Map map = null;
    try {
      Class instanceType = SCOUtils.getContainerInstanceType(ammd.getType(), null);
      map = (Map) instanceType.newInstance();
    } catch (Exception e) {
      throw new NucleusDataStoreException(e.getMessage(), e);
    }

    Class keyType = clr.classForName(ammd.getMap().getKeyType());
    Class valType = clr.classForName(ammd.getMap().getValueType());
    Iterator iter = ((List)value).iterator();
    while (iter.hasNext()) {
      Object listKey = iter.next();
      Object key = listKey;
      Function funcKey = DATASTORE_TO_POJO_TYPE_FUNC.get(keyType);
      if (funcKey != null) {
        key = funcKey.apply(listKey);
      }

      Object listVal = iter.next();
      Object val = listVal;
      Function funcVal = DATASTORE_TO_POJO_TYPE_FUNC.get(valType);
      if (funcVal != null) {
        val = funcVal.apply(listVal);
      }

      map.put(key, val);
    }

    return map;
  }

  /**
   * Converts a datastore blob to either a byte[] or a Byte[].
   *
   * @param shortBlob The short blob to convert.
   * @param pojoType The destination type for the array.
   * @return Object instead of Object[] because primitive arrays don't extend
   * Object[].
   */
  private Object convertShortBlobToByteArray(ShortBlob shortBlob, Class<?> pojoType) {
    if (pojoType.isPrimitive()) {
      return shortBlob.getBytes();
    } else {
      byte[] bytes = shortBlob.getBytes();
      Byte[] array = (Byte[]) Array.newInstance(pojoType, bytes.length);
      for (int i = 0; i < bytes.length; i++) {
        array[i] = bytes[i];
      }
      return array;
    }
  }

  private Collection<Object> newCollection(Class<? extends Collection> collType) {
    Collection<Object> collToReturn;
    if (collType.isInterface()) {
      collToReturn = Utils.newArrayList();
    } else {
      try {
        collToReturn = (Collection<Object>) collType.newInstance();
      } catch (InstantiationException e) {
        throw new NucleusException("Cannot instantiate Collection of type " + collType.getName(), e);
      } catch (IllegalAccessException e) {
        throw new NucleusException("Cannot instantiate Collection of type " + collType.getName(), e);
      }
    }
    return collToReturn;
  }

  /**
   *
   * @param datastoreList Can be null
   * @param pojoType
   * @param collType
   * @return
   */
  Collection<Object> convertDatastoreListToPojoCollection(List<Object> datastoreList, Class<?> pojoType,
      Class<? extends Collection> collType) {
    Function<Object, Object> func = DATASTORE_TO_POJO_TYPE_FUNC.get(pojoType);
    return convertDatastoreListToPojoCollection(datastoreList, pojoType, collType, func);
  }

  /**
   *
   * @param datastoreList Can be null.
   * @param pojoType
   * @param collType
   * @param func
   * @return
   */
  Collection<Object> convertDatastoreListToPojoCollection(List<Object> datastoreList, Class<?> pojoType,
      Class<? extends Collection> collType, Function<Object, Object> func) {
    Collection<Object> listToReturn = newCollection(collType);
    if (datastoreList == null) {
      return listToReturn;
    }
    if (func != null) {
      datastoreList = Utils.transform(datastoreList, func);
    } else if (Enum.class.isAssignableFrom(pojoType)) {
      @SuppressWarnings("unchecked")
      Class<Enum> enumClass = (Class<Enum>) pojoType;
      datastoreList = new ArrayList<Object>(
          Arrays.asList(convertStringListToEnumArray(datastoreList, enumClass)));
    }
    if (collType.isAssignableFrom(datastoreList.getClass())) {
      return datastoreList;
    }
    listToReturn.addAll(datastoreList);
    return listToReturn;
  }

  private Set<Object> newSet(Class<? extends Set> setType) {
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
    return setToReturn;
  }

  /**
   *
   * @param datastoreList Can be null
   * @param pojoType
   * @param setType
   * @param conversionFunc
   * @return
   */
  Set<Object> convertDatastoreListToPojoSet(List<Object> datastoreList, Class<?> pojoType,
      Class<? extends Set> setType, Function<Object, Object> conversionFunc) {
    Collection<?> convertedList =
        convertDatastoreListToPojoCollection(datastoreList, pojoType, ArrayList.class, conversionFunc);
    Set<Object> setToReturn = newSet(setType);
    setToReturn.addAll(convertedList);
    return setToReturn;
  }

  /**
   * Returns Object instead of Object[] because primitive arrays don't extend
   * Object[].
   *
   * @param datastoreList Can be null
   */
  Object convertDatastoreListToPojoArray(List<Object> datastoreList, Class<?> pojoType) {
    Collection<Object> datastoreColl =
        convertDatastoreListToPojoCollection(datastoreList, pojoType, ArrayList.class);
    // We need to see whether we're converting to a primitive array or an
    // array that extends Object[].
    if (pojoType.isPrimitive()) {
      return PrimitiveUtils.COLLECTION_TO_PRIMITIVE_ARRAY_FUNC_MAP.get(pojoType).apply(datastoreColl);
    }
    Object[] array = (Object[]) Array.newInstance(pojoType, datastoreColl.size());
    return datastoreColl.toArray(array);
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
   * Get the datastore to pojo conversion function for the field identified by
   * the given field number, returning the provided default if no conversion
   * function exists.
   */
  private Function<Object, Object> getDatastoreToPojoTypeFunc(
      Function<Object, Object> defaultVal, Class type) {
    Function<Object, Object> candidate = DATASTORE_TO_POJO_TYPE_FUNC.get(type);
    return candidate != null ? candidate : defaultVal;
  }

  /**
   * Get the pojo to datastore conversion function for the field identified by
   * the given field number, returning the provided default if no conversion function exists.
   */
  private Function<Object, Object> getPojoToDatastoreTypeFunc(
      Function<Object, Object> defaultVal, Class type) {
    Function<Object, Object> candidate = pojoToDatastoreTypeFunction.get(type);
    return candidate != null ? candidate : defaultVal;
  }

  private boolean pojoPropertyIsSet(AbstractMemberMetaData ammd) {
    return Set.class.isAssignableFrom(ammd.getType());
  }

  private boolean pojoPropertyIsCollection(AbstractMemberMetaData ammd) {
    return Collection.class.isAssignableFrom(ammd.getType());
  }

  private List<String> convertEnumsToStringList(Iterable<Enum> enums) {
    List<String> result = Utils.newArrayList();
    for (Enum e : enums) {
      result.add(e == null ? null : e.name());
    }
    return result;
  }

    /**
   * Performs type conversions on a pojo value to return the value to be stored.
   * @param typeMgr TypeManager
   * @param clr class loader resolver to use for string to class conversions
   * @param value The pojo value.
   * @param ammd The meta data for the pojo property.
   * @return A representation of the pojo value that can be set on a datastore {@link Entity}.
   */
  @SuppressWarnings("deprecation")
  public Object pojoValueToDatastoreValue(TypeManager typeMgr, ClassLoaderResolver clr, Object value, 
      AbstractMemberMetaData ammd) {
    if (value == null) {
      return null;
    }

    if (ammd.getTypeConverterName() != null) {
      // User-defined type-converter
      TypeConverter conv = typeMgr.getTypeConverterForName(ammd.getTypeConverterName());
      value = conv.toDatastoreType(value);
    } else {
      // Perform conversion
      if (ammd.hasArray()) {
        value = convertPojoArrayToDatastoreValue(ammd, value);
      } else if (ammd.hasCollection()) {
        value = convertPojoCollectionToDatastoreValue(clr, ammd, (Collection<?>) value);
      } else if (ammd.hasMap()) {
        value = convertPojoMapToDatastoreValue(clr, ammd, (Map)value);
      } else {
        if (value != null) {
          if (java.sql.Time.class.isAssignableFrom(ammd.getType())) {
            // java.sql.Time -> Long
            value = ((java.sql.Time)value).getTime(); // Pass a long through since this is not supported
          } else if (java.sql.Date.class.isAssignableFrom(ammd.getType())) {
            // java.sql.Date -> Long
            value = ((java.sql.Date)value).getTime(); // Pass a long through since this is not supported
          } else if (java.sql.Timestamp.class.isAssignableFrom(ammd.getType())) {
            // java.sql.Timestamp -> Long
            value = ((java.sql.Timestamp)value).getTime(); // Pass a long through since this is not supported
          } else if (Date.class.isAssignableFrom(ammd.getType())) {
            // java.util.Date -> Date (date, time, datetime)
            ColumnMetaData[] colmds = ammd.getColumnMetaData();
            if (colmds != null && colmds.length > 0 && colmds[0].getJdbcType() != null) {
              String jdbcType = colmds[0].getJdbcType();
              if (jdbcType.equalsIgnoreCase("time")) { // Dump the date part
                Calendar cal = Calendar.getInstance();
                cal.setTime((Date) value);
                java.sql.Time time = new java.sql.Time(0);
                time.setHours(cal.get(Calendar.HOUR_OF_DAY));
                time.setMinutes(cal.get(Calendar.MINUTE));
                time.setSeconds(cal.get(Calendar.SECOND));
                value = new Date(time.getTime());
              } else if (jdbcType.equalsIgnoreCase("date")) { // Dump the time part
                Calendar cal = Calendar.getInstance();
                cal.setTime((Date) value);
                java.sql.Date date = new java.sql.Date(0);
                date.setDate(cal.get(Calendar.DAY_OF_MONTH));
                date.setMonth(cal.get(Calendar.MONTH));
                date.setYear(cal.get(Calendar.YEAR)-1900);
                value = new Date(date.getTime());
              }
            }
          } else if (Enum.class.isAssignableFrom(ammd.getType())) {
            // Enum -> String(name) / Long(ordinal)
            boolean asOrdinal = false;
            if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length > 0) {
              String jdbcType = ammd.getColumnMetaData()[0].getJdbcType();
              if (jdbcType != null && jdbcType.equalsIgnoreCase("integer")) {
                // Persisted as ordinal
                asOrdinal = true;
              }
            }
            if (asOrdinal) {
              if (value instanceof Enum) {
                value = ((Enum)value).ordinal();
              }
            } else {
              if (value instanceof Enum) {
                value = ((Enum) value).name();
              }
            }
          } else {
            if (supportedClasses.contains(ammd.getType()) ||
                ammd.getTypeName().startsWith("com.google.appengine.api")) {
              value = getPojoToDatastoreTypeFunc(Utils.identity(), ammd.getType()).apply(value);
            } else {
              // Use TypeConverter where appropriate
              // Note : We also come through here when converting parameter values for queries, which may not be the
              // same type as the value of the field (e.g pass in the String form when using a UUID)
              if (ammd.getType().isAssignableFrom(value.getClass())) {
                TypeConverter strConv = typeMgr.getTypeConverterForType(ammd.getType(), String.class);
                if (strConv != null) {
                  // Persist as String
                  value = strConv.toDatastoreType(value);
                } else {
                  TypeConverter longConv = typeMgr.getTypeConverterForType(ammd.getType(), Long.class);
                  if (longConv != null) {
                    // Persist as Long
                    value = longConv.toDatastoreType(value);
                  } else {
                    // Unsupported type on GAE/J ?
                    value = getPojoToDatastoreTypeFunc(Utils.identity(), ammd.getType()).apply(value);
                  }
                }
              }
            }
          }
        }
      }
    }

    return value;
  }

  private Object convertPojoCollectionToDatastoreValue(
      ClassLoaderResolver clr, AbstractMemberMetaData ammd, Collection<?> value) {
    Object result = value;
    Class<?> elementType = clr.classForName(ammd.getCollection().getElementType());
    if (Enum.class.isAssignableFrom(elementType)) {
      @SuppressWarnings("unchecked")
      Iterable<Enum> enums = (Iterable<Enum>) value;
      result = convertEnumsToStringList(enums);
    } else if (pojoPropertyIsByteCollection(ammd)) {
      result = convertByteCollectionToShortBlob((Collection<Byte>) value);
    } else {
      if (pojoToDatastoreTypeFunction.get(elementType) != null) {
        // this will transform non-lists into lists while also transforming
        // the elements of the collection
        result = Utils.transform(value, pojoToDatastoreTypeFunction.get(elementType));
      } else if (!(value instanceof List)){
        // elements don't need to be transformed but the container might
        result = Utils.transform(value, IDENTITY);
      }
    }
    return result;
  }

  private Object convertPojoMapToDatastoreValue(
      ClassLoaderResolver clr, AbstractMemberMetaData ammd, Map value) {
    if (value == null) {
      return null;
    }
    List result = Utils.newArrayList();
    Iterator entryIter = value.entrySet().iterator();
    Class keyType = clr.classForName(ammd.getMap().getKeyType());
    Class valType = clr.classForName(ammd.getMap().getValueType());
    while (entryIter.hasNext()) {
      Map.Entry entry = (Map.Entry)entryIter.next();
      Object key = entry.getKey();
      Function keyFunc = pojoToDatastoreTypeFunction.get(keyType);
      if (keyFunc != null) {
        result.add(keyFunc.apply(key));
      } else {
        result.add(key);
      }
      
      Object val = entry.getValue();
      Function valFunc = pojoToDatastoreTypeFunction.get(valType);
      if (valFunc != null) {
        result.add(valFunc.apply(val));
      } else {
        result.add(val);
      }
    }

    return result;
  }

  private Object convertPojoArrayToDatastoreValue(AbstractMemberMetaData ammd, Object value) {
    Object result;
    if (pojoPropertyIsByteArray(ammd)) {
      result = convertByteArrayToShortBlob(value);
    } else if (Enum.class.isAssignableFrom(ammd.getType().getComponentType())) {
      result = convertEnumsToStringList(Arrays.<Enum>asList((Enum[]) value));
    } else {
      // Translate all arrays to lists before storing.
      result = convertPojoArrayToDatastoreList(value);
    }
    return result;
  }
}
