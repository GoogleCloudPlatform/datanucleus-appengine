// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.users.User;

import org.datanucleus.store.appengine.Utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * A class that contains members of all the types we know how to map.
 *
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class KitchenSink {
  public enum KitchenSinkEnum {ONE, TWO}

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  public String key;

  @Persistent public String strVal;
  @Persistent public Boolean boolVal;
  @Persistent public boolean boolPrimVal;
  @Persistent public Long longVal;
  @Persistent public long longPrimVal;
  @Persistent public Integer integerVal;
  @Persistent public int intVal;
  @Persistent public Character characterVal;
  @Persistent public char charVal;
  @Persistent public Short shortVal;
  @Persistent public short shortPrimVal;
  @Persistent public Byte byteVal;
  @Persistent public byte bytePrimVal;
  @Persistent public Float floatVal;
  @Persistent public float floatPrimVal;
  @Persistent public Double doubleVal;
  @Persistent public double doublePrimVal;
  @Persistent public Date dateVal;
  @Persistent public KitchenSinkEnum ksEnum;
  @Persistent(defaultFetchGroup = "true") public User userVal;
  @Persistent(defaultFetchGroup = "true") public Blob blobVal;
  @Persistent(defaultFetchGroup = "true") public Text textVal;
  @Persistent(defaultFetchGroup = "true") public Link linkVal;

  @Persistent(defaultFetchGroup = "true") public String[] strArray;
  @Persistent(defaultFetchGroup = "true") public int[] primitiveIntArray;
  @Persistent(defaultFetchGroup = "true") public Integer[] integerArray;
  @Persistent(defaultFetchGroup = "true") public long[] primitiveLongArray;
  @Persistent(defaultFetchGroup = "true") public Long[] longArray;
  @Persistent(defaultFetchGroup = "true") public short[] primitiveShortArray;
  @Persistent(defaultFetchGroup = "true") public Short[] shortArray;
  @Persistent(defaultFetchGroup = "true") public char[] primitiveCharArray;
  @Persistent(defaultFetchGroup = "true") public Character[] characterArray;
  @Persistent(defaultFetchGroup = "true") public float[] primitiveFloatArray;
  @Persistent(defaultFetchGroup = "true") public Float[] floatArray;
  @Persistent(defaultFetchGroup = "true") public double[] primitiveDoubleArray;
  @Persistent(defaultFetchGroup = "true") public Double[] doubleArray;
  @Persistent(defaultFetchGroup = "true") public byte[] primitiveByteArray;
  @Persistent(defaultFetchGroup = "true") public Byte[] byteArray;
  @Persistent(defaultFetchGroup = "true") public boolean[] primitiveBooleanArray;
  @Persistent(defaultFetchGroup = "true") public Boolean[] booleanArray;
  @Persistent(defaultFetchGroup = "true") public Date[] dateArray;
  @Persistent(defaultFetchGroup = "true") public KitchenSinkEnum[] ksEnumArray;
  @Persistent(defaultFetchGroup = "true") public User[] userArray;
  @Persistent(defaultFetchGroup = "true") public Blob[] blobArray;
  @Persistent(defaultFetchGroup = "true") public Text[] textArray;
  @Persistent(defaultFetchGroup = "true") public Link[] linkArray;

  @Persistent(defaultFetchGroup = "true") public List<String> strList;
  @Persistent(defaultFetchGroup = "true") public List<Integer> integerList;
  @Persistent(defaultFetchGroup = "true") public List<Long> longList;
  @Persistent(defaultFetchGroup = "true") public List<Short> shortList;
  @Persistent(defaultFetchGroup = "true") public List<Character> charList;
  @Persistent(defaultFetchGroup = "true") public List<Byte> byteList;
  @Persistent(defaultFetchGroup = "true") public List<Double> doubleList;
  @Persistent(defaultFetchGroup = "true") public List<Float> floatList;
  @Persistent(defaultFetchGroup = "true") public List<Boolean> booleanList;
  @Persistent(defaultFetchGroup = "true") public List<Date> dateList;
  @Persistent(defaultFetchGroup = "true") public List<KitchenSinkEnum> ksEnumList;
  @Persistent(defaultFetchGroup = "true") public List<User> userList;
  @Persistent(defaultFetchGroup = "true") public List<Blob> blobList;
  @Persistent(defaultFetchGroup = "true") public List<Text> textList;
  @Persistent(defaultFetchGroup = "true") public List<Link> linkList;

  public static final Date DATE1 = new Date(147);
  public static final Date DATE2 = new Date(247);
  public static final User USER1 = new User("a", "b");
  public static final User USER2 = new User("c", "d");
  public static final Blob BLOB1 = new Blob("a blob".getBytes());
  public static final Blob BLOB2 = new Blob("another blob".getBytes());
  public static final Text TEXT1 = new Text("some text");
  public static final Text TEXT2 = new Text("more text");
  public static final Link LINK1 = new Link("www.google.com");
  public static final Link LINK2 = new Link("www.yahoo.com");

  public static KitchenSink newKitchenSink() {
    return newKitchenSink(null);
  }

  public static KitchenSink newKitchenSink(String key) {
    KitchenSink ks = new KitchenSink();
    ks.key = key;
    ks.strVal = "strVal";
    ks.boolVal = true;
    ks.boolPrimVal = true;
    ks.longVal = 4L;
    ks.longPrimVal = 4L;
    ks.integerVal = 3;
    ks.intVal = 3;
    ks.characterVal = 'a';
    ks.charVal = 'a';
    ks.shortVal = (short) 2;
    ks.shortPrimVal = (short) 2;
    ks.byteVal = 0xb;
    ks.bytePrimVal = 0xb;
    ks.floatVal = 1.01f;
    ks.floatPrimVal = 1.01f;
    ks.doubleVal = 2.22d;
    ks.doublePrimVal = 2.22d;
    ks.dateVal = DATE1;
    ks.ksEnum = KitchenSinkEnum.ONE;
    ks.userVal = USER1;
    ks.blobVal = BLOB1;
    ks.textVal = TEXT1;
    ks.linkVal = LINK1;

    ks.strArray = new String[] {"a", "b"};
    ks.primitiveIntArray = new int[] {1, 2};
    ks.integerArray = new Integer[] {3, 4};
    ks.primitiveLongArray = new long[] {5L, 6L};
    ks.longArray = new Long[] {7L, 8L};
    ks.primitiveShortArray = new short[] {(short) 9, (short) 10};
    ks.shortArray = new Short[] {(short) 11, (short) 12};
    ks.primitiveCharArray = new char[] {'a', 'b'};
    ks.characterArray = new Character[] {'c', 'd'};
    ks.primitiveFloatArray = new float[] {1.01f, 1.02f};
    ks.floatArray = new Float[] {1.03f, 1.04f};
    ks.primitiveDoubleArray = new double[] {2.01d, 2.02d};
    ks.doubleArray = new Double[] {2.03d, 2.04d};
    ks.primitiveByteArray = new byte[] {0xb, 0xc};
    ks.byteArray = new Byte[] {0xe, 0xf};
    ks.primitiveBooleanArray = new boolean[] {true, false};
    ks.booleanArray = new Boolean[] {Boolean.FALSE, Boolean.TRUE};
    ks.dateArray = new Date[] {DATE1, DATE2};
    ks.ksEnumArray = new KitchenSinkEnum[] {KitchenSinkEnum.TWO, KitchenSinkEnum.ONE};
    ks.userArray = new User[] {USER1, USER2};
    ks.blobArray = new Blob[] {BLOB1, BLOB2};
    ks.textArray = new Text[] {TEXT1, TEXT2};
    ks.linkArray = new Link[] {LINK1, LINK2};

    ks.strList = Utils.newArrayList("p", "q");
    ks.integerList = Utils.newArrayList(11, 12);
    ks.longList = Utils.newArrayList(13L, 14L);
    ks.shortList = Utils.newArrayList((short) 15, (short) 16);
    ks.charList = Utils.newArrayList('q', 'r');
    ks.byteList = Utils.newArrayList((byte) 0x8, (byte) 0x9);
    ks.doubleList = Utils.newArrayList(22.44d, 23.55d);
    ks.floatList = Utils.newArrayList(23.44f, 24.55f);
    ks.booleanList = Utils.newArrayList(true, false);
    ks.dateList = Utils.newArrayList(DATE1, DATE2);
    ks.ksEnumList = Utils.newArrayList(KitchenSinkEnum.TWO, KitchenSinkEnum.ONE);
    ks.userList = Utils.newArrayList(USER1, USER2);
    ks.blobList = Utils.newArrayList(BLOB1, BLOB2);
    ks.textList = Utils.newArrayList(TEXT1, TEXT2);
    ks.linkList = Utils.newArrayList(LINK1, LINK2);
    return ks;
  }

  /**
   * Creates a KitchenSink Entity as it would be created by the DatastoreService.
   * This means that ints are longs and floats are doubles.
   */
  public static Entity newKitchenSinkEntity(String keyName, Key parentKey) {
    Entity entity;
    if (keyName != null) {
      entity = new Entity("KitchenSink", keyName, parentKey);
    } else {
      entity = new Entity("KitchenSink", parentKey);
    }
    entity.setProperty("strVal", "strVal");
    entity.setProperty("boolVal", Boolean.TRUE);
    entity.setProperty("boolPrimVal", true);
    entity.setProperty("longVal", 4L);
    entity.setProperty("longPrimVal", 4L);
    entity.setProperty("integerVal", 3L);
    entity.setProperty("intVal", 3L);
    entity.setProperty("characterVal", (long)'a');
    entity.setProperty("charVal", (long)'a');
    entity.setProperty("shortVal", (long)(short) 2);
    entity.setProperty("shortPrimVal", (long) (short) 2);
    entity.setProperty("byteVal", (long) (byte) 0xb);
    entity.setProperty("bytePrimVal", (long) (byte) 0xb);
    entity.setProperty("floatVal", (double) 1.01f);
    entity.setProperty("floatPrimVal", (double) 1.01f);
    entity.setProperty("doubleVal", 2.22d);
    entity.setProperty("doublePrimVal", 2.22d);
    entity.setProperty("doubleVal", 2.22d);
    entity.setProperty("dateVal", DATE1);
    entity.setProperty("ksEnum", KitchenSinkEnum.ONE.name());
    entity.setProperty("userVal", USER1);
    entity.setProperty("blobVal", BLOB1);
    entity.setProperty("textVal", TEXT1);
    entity.setProperty("linkVal", LINK1);
    entity.setProperty("strArray", Utils.newArrayList("a", "b"));
    entity.setProperty("primitiveIntArray", Utils.newArrayList(1L, 2L));
    entity.setProperty("integerArray", Utils.newArrayList(3L, 4L));
    entity.setProperty("primitiveLongArray", Utils.newArrayList(5L, 6L));
    entity.setProperty("longArray", Utils.newArrayList(7L, 8L));
    entity.setProperty("primitiveShortArray", Utils.newArrayList((long)(short) 9, (long)(short) 10));
    entity.setProperty("shortArray", Utils.newArrayList((long)(short) 11, (long)(short) 12));
    entity.setProperty("primitiveCharArray", Utils.newArrayList((long) 'a', (long) 'b'));
    entity.setProperty("characterArray", Utils.newArrayList((long) 'c', (long) 'd'));
    entity.setProperty("primitiveFloatArray", Utils.newArrayList((double) 1.01f, (double) 1.02f));
    entity.setProperty("floatArray", Utils.newArrayList((double) 1.03f, (double) 1.04f));
    entity.setProperty("primitiveDoubleArray", Utils.newArrayList(2.01d, 2.02d));
    entity.setProperty("doubleArray", Utils.newArrayList(2.03d, 2.04d));
    entity.setProperty("primitiveByteArray", new Blob(new byte[] {0xb, 0xc}));
    entity.setProperty("byteArray", new Blob(new byte[] {0xe, 0xf}));
    entity.setProperty("primitiveBooleanArray", Utils.newArrayList(true, false));
    entity.setProperty("booleanArray", Utils.newArrayList(false, true));
    entity.setProperty("dateArray", Utils.newArrayList(DATE1, DATE2));
    entity.setProperty("ksEnumArray",
                       Utils.newArrayList(KitchenSinkEnum.TWO.name(), KitchenSinkEnum.ONE.name()));
    entity.setProperty("userArray", Utils.newArrayList(USER1, USER2));
    entity.setProperty("blobArray", Utils.newArrayList(BLOB1, BLOB2));
    entity.setProperty("textArray", Utils.newArrayList(TEXT1, TEXT2));
    entity.setProperty("linkArray", Utils.newArrayList(LINK1, LINK2));

    entity.setProperty("strList", Utils.newArrayList("p", "q"));
    entity.setProperty("integerList", Utils.newArrayList(11L, 12L));
    entity.setProperty("longList", Utils.newArrayList(13L, 14L));
    entity.setProperty("shortList", Utils.newArrayList((long) (short) 15, (long) (short) 16));
    entity.setProperty("byteList", Utils.newArrayList((long) (byte) 0x8, (long) (byte) 0x9));
    entity.setProperty("charList", Utils.newArrayList((long) 'q', (long) 'r'));
    entity.setProperty("doubleList", Utils.newArrayList(22.44d, 23.55d));
    entity.setProperty("floatList", Utils.newArrayList((double) 23.44f, (double) 24.55f));
    entity.setProperty("booleanList", Utils.newArrayList(true, false));
    entity.setProperty("dateList", Utils.newArrayList(DATE1, DATE2));
    entity.setProperty("ksEnumList",
                       Utils.newArrayList(KitchenSinkEnum.TWO.name(), KitchenSinkEnum.ONE.name()));
    entity.setProperty("userList", Utils.newArrayList(USER1, USER2));
    entity.setProperty("blobList", Utils.newArrayList(BLOB1, BLOB2));
    entity.setProperty("textList", Utils.newArrayList(TEXT1, TEXT2));
    entity.setProperty("linkList", Utils.newArrayList(LINK1, LINK2));
    return entity;
  }

  /**
   * Creates a KitchenSink Entity as it would be created by the DatastoreService.
   * This means that ints are longs and floats are doubles.
   */
  public static Entity newKitchenSinkEntity(Key parentKey) {
    return newKitchenSinkEntity(null, parentKey);
  }

  public static final List<String> KITCHEN_SINK_FIELDS = getKitchenSinkFields();

  private static List<String> getKitchenSinkFields() {
    List<String> fields = Utils.newArrayList();
    for (Field f : KitchenSink.class.getDeclaredFields()) {
      fields.add(f.getName());
    }
    return fields;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KitchenSink that = (KitchenSink) o;

    if (boolPrimVal != that.boolPrimVal) {
      return false;
    }
    if (bytePrimVal != that.bytePrimVal) {
      return false;
    }
    if (charVal != that.charVal) {
      return false;
    }
    if (Double.compare(that.doublePrimVal, doublePrimVal) != 0) {
      return false;
    }
    if (Float.compare(that.floatPrimVal, floatPrimVal) != 0) {
      return false;
    }
    if (intVal != that.intVal) {
      return false;
    }
    if (longPrimVal != that.longPrimVal) {
      return false;
    }
    if (shortPrimVal != that.shortPrimVal) {
      return false;
    }
    if (!Arrays.equals(blobArray, that.blobArray)) {
      return false;
    }
    if (blobList != null ? !blobList.equals(that.blobList) : that.blobList != null) {
      return false;
    }
    if (blobVal != null ? !blobVal.equals(that.blobVal) : that.blobVal != null) {
      return false;
    }
    if (boolVal != null ? !boolVal.equals(that.boolVal) : that.boolVal != null) {
      return false;
    }
    if (!Arrays.equals(booleanArray, that.booleanArray)) {
      return false;
    }
    if (booleanList != null ? !booleanList.equals(that.booleanList) : that.booleanList != null) {
      return false;
    }
    if (!Arrays.equals(byteArray, that.byteArray)) {
      return false;
    }
    if (byteList != null ? !byteList.equals(that.byteList) : that.byteList != null) {
      return false;
    }
    if (byteVal != null ? !byteVal.equals(that.byteVal) : that.byteVal != null) {
      return false;
    }
    if (charList != null ? !charList.equals(that.charList) : that.charList != null) {
      return false;
    }
    if (!Arrays.equals(characterArray, that.characterArray)) {
      return false;
    }
    if (characterVal != null ? !characterVal.equals(that.characterVal)
        : that.characterVal != null) {
      return false;
    }
    if (!Arrays.equals(dateArray, that.dateArray)) {
      return false;
    }
    if (dateList != null ? !dateList.equals(that.dateList) : that.dateList != null) {
      return false;
    }
    if (dateVal != null ? !dateVal.equals(that.dateVal) : that.dateVal != null) {
      return false;
    }
    if (!Arrays.equals(doubleArray, that.doubleArray)) {
      return false;
    }
    if (doubleList != null ? !doubleList.equals(that.doubleList) : that.doubleList != null) {
      return false;
    }
    if (doubleVal != null ? !doubleVal.equals(that.doubleVal) : that.doubleVal != null) {
      return false;
    }
    if (!Arrays.equals(floatArray, that.floatArray)) {
      return false;
    }
    if (floatList != null ? !floatList.equals(that.floatList) : that.floatList != null) {
      return false;
    }
    if (floatVal != null ? !floatVal.equals(that.floatVal) : that.floatVal != null) {
      return false;
    }
    if (!Arrays.equals(integerArray, that.integerArray)) {
      return false;
    }
    if (integerList != null ? !integerList.equals(that.integerList) : that.integerList != null) {
      return false;
    }
    if (integerVal != null ? !integerVal.equals(that.integerVal) : that.integerVal != null) {
      return false;
    }
    if (key != null ? !key.equals(that.key) : that.key != null) {
      return false;
    }
    if (ksEnum != that.ksEnum) {
      return false;
    }
    if (!Arrays.equals(ksEnumArray, that.ksEnumArray)) {
      return false;
    }
    if (ksEnumList != null ? !ksEnumList.equals(that.ksEnumList) : that.ksEnumList != null) {
      return false;
    }
    if (!Arrays.equals(linkArray, that.linkArray)) {
      return false;
    }
    if (linkList != null ? !linkList.equals(that.linkList) : that.linkList != null) {
      return false;
    }
    if (linkVal != null ? !linkVal.equals(that.linkVal) : that.linkVal != null) {
      return false;
    }
    if (!Arrays.equals(longArray, that.longArray)) {
      return false;
    }
    if (longList != null ? !longList.equals(that.longList) : that.longList != null) {
      return false;
    }
    if (longVal != null ? !longVal.equals(that.longVal) : that.longVal != null) {
      return false;
    }
    if (!Arrays.equals(primitiveBooleanArray, that.primitiveBooleanArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveByteArray, that.primitiveByteArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveCharArray, that.primitiveCharArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveDoubleArray, that.primitiveDoubleArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveFloatArray, that.primitiveFloatArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveIntArray, that.primitiveIntArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveLongArray, that.primitiveLongArray)) {
      return false;
    }
    if (!Arrays.equals(primitiveShortArray, that.primitiveShortArray)) {
      return false;
    }
    if (!Arrays.equals(shortArray, that.shortArray)) {
      return false;
    }
    if (shortList != null ? !shortList.equals(that.shortList) : that.shortList != null) {
      return false;
    }
    if (shortVal != null ? !shortVal.equals(that.shortVal) : that.shortVal != null) {
      return false;
    }
    if (!Arrays.equals(strArray, that.strArray)) {
      return false;
    }
    if (strList != null ? !strList.equals(that.strList) : that.strList != null) {
      return false;
    }
    if (strVal != null ? !strVal.equals(that.strVal) : that.strVal != null) {
      return false;
    }
    if (!Arrays.equals(textArray, that.textArray)) {
      return false;
    }
    if (textList != null ? !textList.equals(that.textList) : that.textList != null) {
      return false;
    }
    if (textVal != null ? !textVal.equals(that.textVal) : that.textVal != null) {
      return false;
    }
    if (!Arrays.equals(userArray, that.userArray)) {
      return false;
    }
    if (userList != null ? !userList.equals(that.userList) : that.userList != null) {
      return false;
    }
    if (userVal != null ? !userVal.equals(that.userVal) : that.userVal != null) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    int result;
    long temp;
    result = (key != null ? key.hashCode() : 0);
    result = 31 * result + (strVal != null ? strVal.hashCode() : 0);
    result = 31 * result + (boolVal != null ? boolVal.hashCode() : 0);
    result = 31 * result + (boolPrimVal ? 1 : 0);
    result = 31 * result + (longVal != null ? longVal.hashCode() : 0);
    result = 31 * result + (int) (longPrimVal ^ (longPrimVal >>> 32));
    result = 31 * result + (integerVal != null ? integerVal.hashCode() : 0);
    result = 31 * result + intVal;
    result = 31 * result + (characterVal != null ? characterVal.hashCode() : 0);
    result = 31 * result + (int) charVal;
    result = 31 * result + (shortVal != null ? shortVal.hashCode() : 0);
    result = 31 * result + (int) shortPrimVal;
    result = 31 * result + (byteVal != null ? byteVal.hashCode() : 0);
    result = 31 * result + (int) bytePrimVal;
    result = 31 * result + (floatVal != null ? floatVal.hashCode() : 0);
    result = 31 * result + (floatPrimVal != +0.0f ? Float.floatToIntBits(floatPrimVal) : 0);
    result = 31 * result + (doubleVal != null ? doubleVal.hashCode() : 0);
    temp = doublePrimVal != +0.0d ? Double.doubleToLongBits(doublePrimVal) : 0L;
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + (dateVal != null ? dateVal.hashCode() : 0);
    result = 31 * result + (ksEnum != null ? ksEnum.hashCode() : 0);
    result = 31 * result + (userVal != null ? userVal.hashCode() : 0);
    result = 31 * result + (blobVal != null ? blobVal.hashCode() : 0);
    result = 31 * result + (textVal != null ? textVal.hashCode() : 0);
    result = 31 * result + (linkVal != null ? linkVal.hashCode() : 0);
    result = 31 * result + (strArray != null ? Arrays.hashCode(strArray) : 0);
    result = 31 * result + (primitiveIntArray != null ? Arrays.hashCode(primitiveIntArray) : 0);
    result = 31 * result + (integerArray != null ? Arrays.hashCode(integerArray) : 0);
    result = 31 * result + (primitiveLongArray != null ? Arrays.hashCode(primitiveLongArray) : 0);
    result = 31 * result + (longArray != null ? Arrays.hashCode(longArray) : 0);
    result = 31 * result + (primitiveShortArray != null ? Arrays.hashCode(primitiveShortArray) : 0);
    result = 31 * result + (shortArray != null ? Arrays.hashCode(shortArray) : 0);
    result = 31 * result + (primitiveCharArray != null ? Arrays.hashCode(primitiveCharArray) : 0);
    result = 31 * result + (characterArray != null ? Arrays.hashCode(characterArray) : 0);
    result = 31 * result + (primitiveFloatArray != null ? Arrays.hashCode(primitiveFloatArray) : 0);
    result = 31 * result + (floatArray != null ? Arrays.hashCode(floatArray) : 0);
    result =
        31 * result + (primitiveDoubleArray != null ? Arrays.hashCode(primitiveDoubleArray) : 0);
    result = 31 * result + (doubleArray != null ? Arrays.hashCode(doubleArray) : 0);
    result = 31 * result + (primitiveByteArray != null ? Arrays.hashCode(primitiveByteArray) : 0);
    result = 31 * result + (byteArray != null ? Arrays.hashCode(byteArray) : 0);
    result =
        31 * result + (primitiveBooleanArray != null ? Arrays.hashCode(primitiveBooleanArray) : 0);
    result = 31 * result + (booleanArray != null ? Arrays.hashCode(booleanArray) : 0);
    result = 31 * result + (dateArray != null ? Arrays.hashCode(dateArray) : 0);
    result = 31 * result + (ksEnumArray != null ? Arrays.hashCode(ksEnumArray) : 0);
    result = 31 * result + (userArray != null ? Arrays.hashCode(userArray) : 0);
    result = 31 * result + (blobArray != null ? Arrays.hashCode(blobArray) : 0);
    result = 31 * result + (textArray != null ? Arrays.hashCode(textArray) : 0);
    result = 31 * result + (linkArray != null ? Arrays.hashCode(linkArray) : 0);
    result = 31 * result + (strList != null ? strList.hashCode() : 0);
    result = 31 * result + (integerList != null ? integerList.hashCode() : 0);
    result = 31 * result + (longList != null ? longList.hashCode() : 0);
    result = 31 * result + (shortList != null ? shortList.hashCode() : 0);
    result = 31 * result + (charList != null ? charList.hashCode() : 0);
    result = 31 * result + (byteList != null ? byteList.hashCode() : 0);
    result = 31 * result + (doubleList != null ? doubleList.hashCode() : 0);
    result = 31 * result + (floatList != null ? floatList.hashCode() : 0);
    result = 31 * result + (booleanList != null ? booleanList.hashCode() : 0);
    result = 31 * result + (dateList != null ? dateList.hashCode() : 0);
    result = 31 * result + (ksEnumList != null ? ksEnumList.hashCode() : 0);
    result = 31 * result + (userList != null ? userList.hashCode() : 0);
    result = 31 * result + (blobList != null ? blobList.hashCode() : 0);
    result = 31 * result + (textList != null ? textList.hashCode() : 0);
    result = 31 * result + (linkList != null ? linkList.hashCode() : 0);
    return result;
  }
}
