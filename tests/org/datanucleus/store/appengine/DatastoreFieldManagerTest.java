// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.test.KitchenSink;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.JDOClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.easymock.EasyMock;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Blob;
import com.google.apphosting.api.datastore.Text;
import com.google.apphosting.api.datastore.Link;
import com.google.apphosting.api.users.User;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.Date;
import java.util.Arrays;
import java.lang.reflect.Field;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManagerTest extends JDOTestCase {

  public void testFetching() throws ClassNotFoundException {
    Entity ksEntity = KitchenSink.newKitchenSinkEntity(null);
    ldth.ds.put(ksEntity);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(KitchenSink.class, clr);
    DatastoreFieldManager fieldManager = new DatastoreFieldManager(null, ksEntity) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }

      @Override
      IdentifierFactory getIdentifierFactory() {
        return DatastoreFieldManagerTest.this.getIdentifierFactory();
      }
    };

    FieldPositionIterator iter = new FieldPositionIterator(acmd);
    assertEquals(KeyFactory.encodeKey(ksEntity.getKey()),fieldManager.fetchStringField(iter.next()));
    assertEquals("strVal", fieldManager.fetchStringField(iter.next()));
    assertEquals(true, fieldManager.fetchBooleanField(iter.next()));
    assertEquals(true, fieldManager.fetchBooleanField(iter.next()));
    assertEquals(4L, fieldManager.fetchLongField(iter.next()));
    assertEquals(4L, fieldManager.fetchLongField(iter.next()));
    assertEquals(3, fieldManager.fetchIntField(iter.next()));
    assertEquals(3, fieldManager.fetchIntField(iter.next()));
    assertEquals('a', fieldManager.fetchCharField(iter.next()));
    assertEquals('a', fieldManager.fetchCharField(iter.next()));
    assertEquals((short) 2, fieldManager.fetchShortField(iter.next()));
    assertEquals((short) 2, fieldManager.fetchShortField(iter.next()));
    assertEquals((byte) 0xb, fieldManager.fetchByteField(iter.next()));
    assertEquals((byte) 0xb, fieldManager.fetchByteField(iter.next()));
    assertEquals(1.01f, fieldManager.fetchFloatField(iter.next()));
    assertEquals(1.01f, fieldManager.fetchFloatField(iter.next()));
    assertEquals(2.22d, fieldManager.fetchDoubleField(iter.next()));
    assertEquals(2.22d, fieldManager.fetchDoubleField(iter.next()));
    assertEquals(KitchenSink.DATE1, fieldManager.fetchObjectField(iter.next()));
    assertEquals(KitchenSink.USER1, fieldManager.fetchObjectField(iter.next()));
    assertEquals(KitchenSink.BLOB1, fieldManager.fetchObjectField(iter.next()));
    assertEquals(KitchenSink.TEXT1, fieldManager.fetchObjectField(iter.next()));
    assertEquals(KitchenSink.LINK1, fieldManager.fetchObjectField(iter.next()));
    assertTrue(Arrays.equals(new String[] {"a", "b"},
        (String[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new int[] {1, 2}, (int[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Integer[] {3, 4},
        (Integer[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new long[] {5L, 6L},
        (long[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Long[] {7L, 8L},
        (Long[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new short[] {(short) 9, (short) 10},
        (short[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Short[] {(short) 11, (short) 12},
        (Short[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new char[] {'a', 'b'},
        (char[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Character[] {'c', 'd'},
        (Character[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new float[] {1.01f, 1.02f},
        (float[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Float[] {1.03f, 1.04f},
        (Float[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new double[] {2.01d, 2.02d},
        (double[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Double[] {2.03d, 2.04d},
        (Double[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new byte[] {0xb, 0xc},
        (byte[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Byte[] {0xe, 0xf},
        (Byte[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new boolean[] {true, false},
        (boolean[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Boolean[] {Boolean.FALSE, Boolean.TRUE},
        (Boolean[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Date[] {KitchenSink.DATE1, KitchenSink.DATE2},
        (Date[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new User[] {KitchenSink.USER1, KitchenSink.USER2},
        (User[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Blob[] {KitchenSink.BLOB1, KitchenSink.BLOB2},
        (Blob[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Text[] {KitchenSink.TEXT1, KitchenSink.TEXT2},
        (Text[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Link[] {KitchenSink.LINK1, KitchenSink.LINK2},
        (Link[]) fieldManager.fetchObjectField(iter.next())));

    assertEquals(Lists.newArrayList("p", "q"), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(11, 12), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(13L, 14L), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList((short) 15, (short) 16),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList('q', 'r'), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList((byte) 0x8, (byte) 0x9),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(22.44d, 23.55d), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(23.44f, 24.55f), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(true, false), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(KitchenSink.DATE1, KitchenSink.DATE2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(KitchenSink.USER1, KitchenSink.USER2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(KitchenSink.BLOB1, KitchenSink.BLOB2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(KitchenSink.TEXT1, KitchenSink.TEXT2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Lists.newArrayList(KitchenSink.LINK1, KitchenSink.LINK2),
        fieldManager.fetchObjectField(iter.next()));

  }

  public void testStorage() throws ClassNotFoundException {
    Entity ksEntity = new Entity("KitchenSink");
    ldth.ds.put(ksEntity);

    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(KitchenSink.class, clr);
    DatastoreFieldManager fieldManager = new DatastoreFieldManager(null, ksEntity) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }

      @Override
      IdentifierFactory getIdentifierFactory() {
        return DatastoreFieldManagerTest.this.getIdentifierFactory();
      }
    };
    FieldPositionIterator iter = new FieldPositionIterator(acmd);
    // skip the key field because storing it doesn't do anything
    iter.next();
    fieldManager.storeStringField(iter.next(), "strVal");
    fieldManager.storeObjectField(iter.next(), Boolean.TRUE);
    fieldManager.storeBooleanField(iter.next(), true);
    fieldManager.storeObjectField(iter.next(), 4L);
    fieldManager.storeLongField(iter.next(), 4L);
    fieldManager.storeObjectField(iter.next(), 3);
    fieldManager.storeIntField(iter.next(), 3);
    fieldManager.storeObjectField(iter.next(), 'a');
    fieldManager.storeCharField(iter.next(), 'a');
    fieldManager.storeObjectField(iter.next(), (short) 2);
    fieldManager.storeShortField(iter.next(), (short) 2);
    fieldManager.storeObjectField(iter.next(), (byte) 0xb);
    fieldManager.storeByteField(iter.next(), (byte) 0xb);
    fieldManager.storeObjectField(iter.next(), 1.01f);
    fieldManager.storeFloatField(iter.next(), 1.01f);
    fieldManager.storeObjectField(iter.next(), 2.22d);
    fieldManager.storeDoubleField(iter.next(), 2.22d);
    fieldManager.storeObjectField(iter.next(), KitchenSink.DATE1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.USER1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.BLOB1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.TEXT1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.LINK1);

    Iterator<Field> fieldIter = Arrays.asList(KitchenSink.class.getDeclaredFields()).iterator();
    fieldIter.next(); // skip the key field
    assertEquals("strVal", ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(true, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(true, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(4L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(4L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(3, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(3, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(97L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(97L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals((short) 2, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals((short) 2, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals((byte) 11, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals((byte) 11, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(1.01f, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(1.01f, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2.22d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2.22d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.DATE1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.USER1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.BLOB1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.TEXT1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.LINK1, ksEntity.getProperty(fieldIter.next().getName()));
  }

  public void testAncestorValues() throws ClassNotFoundException {
    Entity entity = new Entity("my kind");
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(HasAncestorJDO.class, clr);
    StateManager sm = EasyMock.createMock(StateManager.class);
    EasyMock.expect(sm.getClassMetaData()).andReturn(acmd);
    EasyMock.replay(sm);
    DatastoreFieldManager fieldManager = new DatastoreFieldManager(sm, entity) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }
    };

    FieldPositionIterator iter = new FieldPositionIterator(acmd);
    int ancestorPkFieldPos = iter.next();
    try {
      fieldManager.storeStringField(ancestorPkFieldPos, null);
      fail("Expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }
    try {
      fieldManager.storeStringField(ancestorPkFieldPos, "a val");
      fail("Expected ise");
    } catch (IllegalStateException ise) {
      // good
    }

    // now we create a field manager where we don't provide the entity
    // in the constructor
    fieldManager = new DatastoreFieldManager(sm, "my kind") {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }
    };

    // null value is still not ok
    try {
      fieldManager.storeStringField(ancestorPkFieldPos, null);
      fail("Expected iae");
    } catch (IllegalArgumentException iae) {
      // good
    }

    Entity ksEntity = KitchenSink.newKitchenSinkEntity(null);
    ldth.ds.put(ksEntity);
    // non-null value is ok
    fieldManager.storeStringField(ancestorPkFieldPos, KeyFactory.encodeKey(ksEntity.getKey()));
    Entity newEntity = fieldManager.getEntity();
    assertEquals(ksEntity.getKey(), newEntity.getParent());
  }

  private static final class FieldPositionIterator implements Iterator<Integer> {
    private final AbstractClassMetaData acmd;
    private final Iterator<Field> inner;

    private FieldPositionIterator(AbstractClassMetaData acmd) throws ClassNotFoundException {
      this.acmd = acmd;
      this.inner =
          Arrays.asList(Class.forName(acmd.getFullClassName()).getDeclaredFields()).iterator();
    }

    public boolean hasNext() {
      return inner.hasNext();
    }

    public Integer next() {
      return acmd.getRelativePositionOfMember(inner.next().getName());
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private IdentifierFactory getIdentifierFactory() {
    return ((MappedStoreManager)((JDOPersistenceManagerFactory)pmf).getOMFContext()
        .getStoreManager()).getIdentifierFactory();
  }

}
