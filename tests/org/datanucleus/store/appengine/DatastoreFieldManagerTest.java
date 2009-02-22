// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.users.User;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.JDOClassLoaderResolver;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.test.HasAncestorJDO;
import org.datanucleus.test.KitchenSink;
import org.easymock.EasyMock;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreFieldManagerTest extends JDOTestCase {

  private static final StateManager DUMMY_STATE_MANAGER =
      (StateManager) Proxy.newProxyInstance(
          DatastoreFieldManagerTest.class.getClassLoader(),
          new Class[] {StateManager.class},
          new InvocationHandler() {
            public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
              throw new UnsupportedOperationException();
            }
          });

  public void testFetching() {
    Entity ksEntity = KitchenSink.newKitchenSinkEntity(null);
    ldth.ds.put(ksEntity);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    final ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(KitchenSink.class, clr);
    final TypeConversionUtils tcu = new TypeConversionUtils() {
      @Override
      Object wrap(StateManager ownerSM, AbstractMemberMetaData ammd, Object value) {
        return value;
      }
    };
    DatastoreFieldManager fieldManager =
        new DatastoreFieldManager(DUMMY_STATE_MANAGER, getStoreManager(), ksEntity, new int[0]) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }

      @Override
      ClassLoaderResolver getClassLoaderResolver() {
        return clr;
      }

      @Override
      TypeConversionUtils getConversionUtils() {
        return tcu;
      }
    };

    FieldPositionIterator iter = new FieldPositionIterator(acmd);
    assertEquals(KeyFactory.keyToString(ksEntity.getKey()),fieldManager.fetchStringField(iter.next()));
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
    assertEquals(KitchenSink.KitchenSinkEnum.ONE, fieldManager.fetchObjectField(iter.next()));
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
    assertTrue(Arrays.equals(new KitchenSink.KitchenSinkEnum[]
        {KitchenSink.KitchenSinkEnum.TWO, KitchenSink.KitchenSinkEnum.ONE},
        (KitchenSink.KitchenSinkEnum[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new User[] {KitchenSink.USER1, KitchenSink.USER2},
        (User[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Blob[] {KitchenSink.BLOB1, KitchenSink.BLOB2},
        (Blob[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Text[] {KitchenSink.TEXT1, KitchenSink.TEXT2},
        (Text[]) fieldManager.fetchObjectField(iter.next())));
    assertTrue(Arrays.equals(new Link[] {KitchenSink.LINK1, KitchenSink.LINK2},
        (Link[]) fieldManager.fetchObjectField(iter.next())));

    assertEquals(Utils.newArrayList("p", "q"), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(11, 12), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(13L, 14L), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList((short) 15, (short) 16),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList('q', 'r'), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList((byte) 0x8, (byte) 0x9),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(22.44d, 23.55d), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(23.44f, 24.55f), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(true, false), fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.DATE1, KitchenSink.DATE2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.KitchenSinkEnum.TWO, KitchenSink.KitchenSinkEnum.ONE),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.USER1, KitchenSink.USER2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.BLOB1, KitchenSink.BLOB2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.TEXT1, KitchenSink.TEXT2),
        fieldManager.fetchObjectField(iter.next()));
    assertEquals(Utils.newArrayList(KitchenSink.LINK1, KitchenSink.LINK2),
        fieldManager.fetchObjectField(iter.next()));
  }

  public void testFetchingNullsForNotNullFields() {
    Entity entity = new Entity(KitchenSink.class.getSimpleName());
    ldth.ds.put(entity);
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    final ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(KitchenSink.class, clr);
    DatastoreFieldManager fieldManager =
        new DatastoreFieldManager(DUMMY_STATE_MANAGER, getStoreManager(), entity, new int[0]) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }

      @Override
      ClassLoaderResolver getClassLoaderResolver() {
        return clr;
      }
    };
    try {
      fieldManager.fetchBooleanField(acmd.getRelativePositionOfMember("boolPrimVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchLongField(acmd.getRelativePositionOfMember("longPrimVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchIntField(acmd.getRelativePositionOfMember("intVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchFloatField(acmd.getRelativePositionOfMember("floatPrimVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchDoubleField(acmd.getRelativePositionOfMember("doublePrimVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchByteField(acmd.getRelativePositionOfMember("bytePrimVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }

    try {
      fieldManager.fetchCharField(acmd.getRelativePositionOfMember("charVal"));
      fail("expected npe");
    } catch (NullPointerException npe) {
      // good
    }
  }

  public void testStorage() {
    Entity ksEntity = new Entity("KitchenSink");
    ldth.ds.put(ksEntity);

    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    final ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(KitchenSink.class, clr);
    DatastoreFieldManager fieldManager =
        new DatastoreFieldManager(DUMMY_STATE_MANAGER, getStoreManager(), ksEntity, new int[0]) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }

      @Override
      ClassLoaderResolver getClassLoaderResolver() {
        return clr;
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
    fieldManager.storeObjectField(iter.next(), KitchenSink.KitchenSinkEnum.ONE);
    fieldManager.storeObjectField(iter.next(), KitchenSink.USER1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.BLOB1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.TEXT1);
    fieldManager.storeObjectField(iter.next(), KitchenSink.LINK1);

    fieldManager.storeObjectField(iter.next(), new String[] {"a", "b"});
    fieldManager.storeObjectField(iter.next(), new int[] {1, 2});
    fieldManager.storeObjectField(iter.next(), new Integer[] {3, 4});
    fieldManager.storeObjectField(iter.next(), new long[] {5L, 6L});
    fieldManager.storeObjectField(iter.next(), new Long[] {7L, 8L});
    fieldManager.storeObjectField(iter.next(), new short[] {(short) 9, (short) 10});
    fieldManager.storeObjectField(iter.next(), new Short[] {(short) 11, (short) 12});
    fieldManager.storeObjectField(iter.next(), new char[] {'a', 'b'});
    fieldManager.storeObjectField(iter.next(), new Character[] {'c', 'd'});
    fieldManager.storeObjectField(iter.next(), new float[] {1.01f, 1.02f});
    fieldManager.storeObjectField(iter.next(), new Float[] {1.03f, 1.04f});
    fieldManager.storeObjectField(iter.next(), new double[] {2.01d, 2.02d});
    fieldManager.storeObjectField(iter.next(), new Double[] {2.03d, 2.04d});
    fieldManager.storeObjectField(iter.next(), new byte[] {0xb, 0xc});
    fieldManager.storeObjectField(iter.next(), new Byte[] {0xe, 0xf});
    fieldManager.storeObjectField(iter.next(), new boolean[] {true, false});
    fieldManager.storeObjectField(iter.next(), new Boolean[] {Boolean.FALSE, Boolean.TRUE});
    fieldManager.storeObjectField(iter.next(), new Date[] {KitchenSink.DATE1, KitchenSink.DATE2});
    fieldManager.storeObjectField(iter.next(), new KitchenSink.KitchenSinkEnum[]
        {KitchenSink.KitchenSinkEnum.TWO, KitchenSink.KitchenSinkEnum.ONE});
    fieldManager.storeObjectField(iter.next(), new User[] {KitchenSink.USER1, KitchenSink.USER2});
    fieldManager.storeObjectField(iter.next(), new Blob[] {KitchenSink.BLOB1, KitchenSink.BLOB2});
    fieldManager.storeObjectField(iter.next(), new Text[] {KitchenSink.TEXT1, KitchenSink.TEXT2});
    fieldManager.storeObjectField(iter.next(), new Link[] {KitchenSink.LINK1, KitchenSink.LINK2});

    fieldManager.storeObjectField(iter.next(), Utils.newArrayList("p", "q"));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList(11, 12));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList(13L, 14L));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList((short) 15, (short) 16));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList('q', 'r'));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList((byte) 0x8, (byte) 0x9));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList(22.44d, 23.55d));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList(23.44f, 24.55f));
    fieldManager.storeObjectField(iter.next(), Utils.newArrayList(true, false));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.DATE1, KitchenSink.DATE2));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.KitchenSinkEnum.TWO, KitchenSink.KitchenSinkEnum.ONE));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.USER1, KitchenSink.USER2));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.BLOB1, KitchenSink.BLOB2));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.TEXT1, KitchenSink.TEXT2));
    fieldManager.storeObjectField(iter.next(),
                                  Utils.newArrayList(KitchenSink.LINK1, KitchenSink.LINK2));

    Iterator<Field> fieldIter = Arrays.asList(KitchenSink.class.getDeclaredFields()).iterator();
    fieldIter.next(); // skip the key field
    assertEquals("strVal", ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(true, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(true, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(4L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(4L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(3L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(3L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(97L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(97L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(11L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(11L, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(1.0099999904632568d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(1.0099999904632568d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2.22d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(2.22d, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.DATE1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.KitchenSinkEnum.ONE.name(),
                 ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.USER1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.BLOB1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.TEXT1, ksEntity.getProperty(fieldIter.next().getName()));
    assertEquals(KitchenSink.LINK1, ksEntity.getProperty(fieldIter.next().getName()));
  }

  public void testAncestorValues() {
    Entity entity = new Entity(HasAncestorJDO.class.getSimpleName());
    JDOPersistenceManager jpm = (JDOPersistenceManager) pm;
    ClassLoaderResolver clr = new JDOClassLoaderResolver();
    final AbstractClassMetaData acmd =
        jpm.getObjectManager().getMetaDataManager().getMetaDataForClass(HasAncestorJDO.class, clr);
    StateManager sm = EasyMock.createMock(StateManager.class);
    ObjectManager om = EasyMock.createMock(ObjectManager.class);
    EasyMock.expect(sm.getObjectManager()).andReturn(om).anyTimes();
    EasyMock.replay(sm);
    EasyMock.expect(om.getClassLoaderResolver()).andReturn(clr).anyTimes();
    EasyMock.expect(om.getStoreManager()).andReturn(getStoreManager()).anyTimes();
    EasyMock.replay(om);
    DatastoreFieldManager fieldManager = new DatastoreFieldManager(sm, getStoreManager(), entity, new int[0]) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }
    };

    FieldPositionIterator iter = new FieldPositionIterator(acmd);
    int ancestorPkFieldPos = iter.next();
    // null ancestor value is fine
    fieldManager.storeStringField(ancestorPkFieldPos, null);
    assertNull(entity.getParent());

    // non-null ancestor value is not fine because we created our own
    // entity.
    try {
      fieldManager.storeStringField(
          ancestorPkFieldPos, KeyFactory.keyToString(KeyFactory.createKey("yar", 44)));
      fail("Expected exception");
    } catch (NucleusUserException e) {
      // good
    }

    // now we create a field manager where we don't provide the entity
    // in the constructor
    fieldManager = new DatastoreFieldManager(
        sm, HasAncestorJDO.class.getSimpleName(), getStoreManager()) {
      @Override
      AbstractClassMetaData getClassMetaData() {
        return acmd;
      }
    };

    // null value is ok
    fieldManager.storeStringField(ancestorPkFieldPos, null);
    assertNull(entity.getParent());

    Entity ksEntity = KitchenSink.newKitchenSinkEntity(null);
    ldth.ds.put(ksEntity);

    // non-null value is ok
    fieldManager.storeStringField(ancestorPkFieldPos, KeyFactory.keyToString(ksEntity.getKey()));
    Entity newEntity = fieldManager.getEntity();
    assertEquals(ksEntity.getKey(), newEntity.getParent());
  }

  private static final class FieldPositionIterator implements Iterator<Integer> {
    private final AbstractClassMetaData acmd;
    private final Iterator<Field> inner;

    private FieldPositionIterator(AbstractClassMetaData acmd) {
      this.acmd = acmd;
      try {
        this.inner =
            Arrays.asList(Class.forName(acmd.getFullClassName()).getDeclaredFields()).iterator();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
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

  private DatastoreManager getStoreManager() {
    return
        ((DatastoreManager)((JDOPersistenceManagerFactory)pmf).getOMFContext().getStoreManager());
  }
}
