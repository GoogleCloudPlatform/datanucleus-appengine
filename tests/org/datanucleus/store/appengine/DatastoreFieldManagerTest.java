// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import junit.framework.TestCase;
import org.datanucleus.test.KitchenSink;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Blob;
import com.google.apphosting.api.datastore.Text;
import com.google.apphosting.api.datastore.Link;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.users.User;

import java.lang.reflect.Field;
import java.util.Date;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManagerTest extends TestCase {

  private static final int PK_FIELD_NUM = 0;
  private static Field[] fields = KitchenSink.class.getDeclaredFields();

  private static final Date A_DATE = new Date(147);
  private static final User A_USER = new User("a", "b");
  private static final Blob A_BLOB = new Blob("a blob".getBytes());
  private static final Text A_TEXT = new Text("some text");
  private static final Link A_LINK = new Link("www.google.com");

  private LocalDatastoreTestHelper ldth = new LocalDatastoreTestHelper();

  protected void setUp() throws Exception {
    super.setUp();
    ldth.setUp();
  }

  protected void tearDown() throws Exception {
    ldth.tearDown();
    super.tearDown();
  }

  public void testFetching() {
    Entity ksEntity = buildKitchenSinkEntity(null);
    ldth.ds.put(ksEntity);
    AppEngineFieldManager fieldManager = new AppEngineFieldManager(null, ksEntity) {
      boolean isPK(int fieldNumber) {
        return fieldNumber == PK_FIELD_NUM;
      }

      String getFieldName(int fieldNumber) {
        return fields[fieldNumber].getName();
      }
    };
    int i = 1;
    assertEquals(KeyFactory.encodeKey(ksEntity.getKey()), fieldManager.fetchStringField(PK_FIELD_NUM));
    assertEquals("strVal", fieldManager.fetchStringField(i++));
    assertEquals(true, fieldManager.fetchBooleanField(i++));
    assertEquals(true, fieldManager.fetchBooleanField(i++));
    assertEquals(4L, fieldManager.fetchLongField(i++));
    assertEquals(4L, fieldManager.fetchLongField(i++));
    assertEquals(3, fieldManager.fetchIntField(i++));
    assertEquals(3, fieldManager.fetchIntField(i++));
    assertEquals('a', fieldManager.fetchCharField(i++));
    assertEquals('a', fieldManager.fetchCharField(i++));
    assertEquals((short) 2, fieldManager.fetchShortField(i++));
    assertEquals((short) 2, fieldManager.fetchShortField(i++));
    assertEquals((byte) 0xb, fieldManager.fetchByteField(i++));
    assertEquals((byte) 0xb, fieldManager.fetchByteField(i++));
    assertEquals(1.01f, fieldManager.fetchFloatField(i++));
    assertEquals(1.01f, fieldManager.fetchFloatField(i++));
    assertEquals(2.22d, fieldManager.fetchDoubleField(i++));
    assertEquals(2.22d, fieldManager.fetchDoubleField(i++));
    assertEquals(A_DATE, fieldManager.fetchObjectField(i++));
    assertEquals(A_USER, fieldManager.fetchObjectField(i++));
    assertEquals(A_BLOB, fieldManager.fetchObjectField(i++));
    assertEquals(A_TEXT, fieldManager.fetchObjectField(i++));
    assertEquals(A_LINK, fieldManager.fetchObjectField(i++));
  }

  public void testFetchingNulls() {
  }

  private Entity buildKitchenSinkEntity(Key key) {
    Entity entity = new Entity("KitchenSink", key);
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
    entity.setProperty("dateVal", A_DATE);
    entity.setProperty("userVal", A_USER);
    entity.setProperty("blobVal", A_BLOB);
    entity.setProperty("textVal", A_TEXT);
    entity.setProperty("linkVal", A_LINK);
    return entity;
  }

  private KitchenSink buildKitchenSink() {
    KitchenSink ks = new KitchenSink();
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
    ks.dateVal = A_DATE;
    ks.userVal = A_USER;
    ks.blobVal = A_BLOB;
    ks.textVal = A_TEXT;
    ks.linkVal = A_LINK;
    return ks;
  }

  public void testStorage() {
    Entity ks = new Entity("KitchenSink");
    ldth.ds.put(ks);

    AppEngineFieldManager fieldManager = new AppEngineFieldManager(null, ks) {
      boolean isPK(int fieldNumber) {
        return fieldNumber == PK_FIELD_NUM;
      }

      String getFieldName(int fieldNumber) {
        return fields[fieldNumber].getName();
      }
    };
    int i = 1;
    fieldManager.storeStringField(PK_FIELD_NUM, KeyFactory.encodeKey(ks.getKey()));
    fieldManager.storeStringField(i++, "strVal");
    fieldManager.storeBooleanField(i++, Boolean.TRUE);
    fieldManager.storeBooleanField(i++, true);
    fieldManager.storeLongField(i++, 4L);
    fieldManager.storeLongField(i++, 4L);
    fieldManager.storeIntField(i++, 3);
    fieldManager.storeIntField(i++, 3);
    fieldManager.storeCharField(i++, 'a');
    fieldManager.storeCharField(i++, 'a');
    fieldManager.storeShortField(i++, (short) 2);
    fieldManager.storeShortField(i++, (short) 2);
    fieldManager.storeByteField(i++, (byte) 0xb);
    fieldManager.storeByteField(i++, (byte) 0xb);
    fieldManager.storeFloatField(i++, 1.01f);
    fieldManager.storeFloatField(i++, 1.01f);
    fieldManager.storeDoubleField(i++, 2.22d);
    fieldManager.storeDoubleField(i++, 2.22d);
    fieldManager.storeObjectField(i++, A_DATE);
    fieldManager.storeObjectField(i++, A_USER);
    fieldManager.storeObjectField(i++, A_BLOB);
    fieldManager.storeObjectField(i++, A_TEXT);
    fieldManager.storeObjectField(i++, A_LINK);

    assertEquals(ks.getKey(), ks.getKey());
    assertEquals("strVal", ks.getProperty("strVal"));
    assertEquals(true, ks.getProperty("boolVal"));
    assertEquals(true, ks.getProperty("boolPrimVal"));
    assertEquals(4L, ks.getProperty("longVal"));
    assertEquals(4L, ks.getProperty("longPrimVal"));
    assertEquals(3L, ks.getProperty("integerVal"));
    assertEquals(3L, ks.getProperty("intVal"));
    assertEquals(97L, ks.getProperty("characterVal"));
    assertEquals(97L, ks.getProperty("charVal"));
    assertEquals(2L, ks.getProperty("shortVal"));
    assertEquals(2L, ks.getProperty("shortPrimVal"));
    assertEquals(11L, ks.getProperty("byteVal"));
    assertEquals(11L, ks.getProperty("bytePrimVal"));
    assertEquals((double) 1.01f, ks.getProperty("floatVal"));
    assertEquals((double) 1.01f, ks.getProperty("floatPrimVal"));
    assertEquals(2.22d, ks.getProperty("doubleVal"));
    assertEquals(2.22d, ks.getProperty("doublePrimVal"));
    assertEquals(A_DATE, ks.getProperty("dateVal"));
    assertEquals(A_USER, ks.getProperty("userVal"));
    assertEquals(A_BLOB, ks.getProperty("blobVal"));
    assertEquals(A_TEXT, ks.getProperty("textVal"));
    assertEquals(A_LINK, ks.getProperty("linkVal"));

  }
}
