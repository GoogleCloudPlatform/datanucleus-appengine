// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.Blob;
import com.google.apphosting.api.datastore.Text;
import com.google.apphosting.api.datastore.Link;
import com.google.apphosting.api.users.User;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Persistent;
import java.util.Date;
import java.util.List;

/**
 * A class that contains members of all the types we know how to map.
 *
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.DATASTORE)
public class KitchenSink {
  @PrimaryKey public Key key;

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
  @Persistent public User userVal;
  @Persistent public Blob blobVal;
  @Persistent public Text textVal;
  @Persistent public Link linkVal;

  @Persistent public String[] strArray;
  @Persistent public int[] primitiveIntArray;
  @Persistent public Integer[] integerArray;
  @Persistent public long[] primitiveLongArray;
  @Persistent public Long[] longArray;
  @Persistent public float[] primitiveFloatArray;
  @Persistent public Float[] floatArray;
  @Persistent public Link[] linkArray;

  @Persistent public List<String> strList;
  @Persistent public List<Integer> integerList;
  @Persistent public List<Long> longList;
  @Persistent public List<Float> floatList;
  @Persistent public List<Double> doubleList;
  @Persistent public List<Link> linkList;
}
