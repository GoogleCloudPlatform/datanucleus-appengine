// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

/**
 * @author Max Ross <maxr@google.com>
 */
public final class TestUtils {

  private TestUtils() {}

  public static void assertKeyParentEquals(String parentKey, Entity childEntity, Key childKey) {
    assertEquals(KeyFactory.stringToKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.stringToKey(parentKey), childKey.getParent());
  }

  public static void assertKeyParentEquals(String parentKey, Entity childEntity, String childKey) {
    assertEquals(KeyFactory.stringToKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.stringToKey(parentKey), KeyFactory.stringToKey(childKey).getParent());
  }

  public static void assertKeyParentEquals(Class<?> parentClass, long parentId, Entity childEntity, Key childKey) {
    Key parentKey = TestUtils.createKey(parentClass, parentId);
    assertEquals(parentKey, childEntity.getKey().getParent());
    assertEquals(parentKey, childKey.getParent());
  }

  public static void assertKeyParentEquals(Class<?> parentClass, long parentId, Entity childEntity, String childKey) {
    Key parentKey = TestUtils.createKey(parentClass, parentId);
    assertEquals(parentKey, childEntity.getKey().getParent());
    assertEquals(parentKey, KeyFactory.stringToKey(childKey).getParent());
  }

  public static void assertKeyParentEquals(Class<?> parentClass, String parentId, Entity childEntity, Key childKey) {
    Key parentKey = TestUtils.createKey(parentClass, parentId);
    assertEquals(parentKey, childEntity.getKey().getParent());
    assertEquals(parentKey, childKey.getParent());
  }

  public static void assertKeyParentEquals(Class<?> parentClass, String parentId, Entity childEntity, String childKey) {
    Key parentKey = TestUtils.createKey(parentClass, parentId);
    assertEquals(parentKey, childEntity.getKey().getParent());
    assertEquals(parentKey, KeyFactory.stringToKey(childKey).getParent());
  }

  public static void assertKeyParentNull(Entity childEntity, String childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(KeyFactory.stringToKey(childKey).getParent());
  }

  public static void assertKeyParentNull(Entity childEntity, Key childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(childKey.getParent());
  }

  public static Key createKey(Object pojo, long id) {
    return createKey(pojo.getClass(), id);
  }

  public static Key createKey(Class<?> clazz, long id) {
    return KeyFactory.createKey(clazz.getSimpleName(), id);
  }

  public static Key createKey(Object pojo, String name) {
    return KeyFactory.createKey(pojo.getClass().getSimpleName(), name);
  }

  public static String createKeyString(Object pojo, String name) {
    return createKeyString(pojo.getClass(), name);
  }

  public static String createKeyString(Class<?> clazz, String name) {
    return KeyFactory.keyToString(createKey(clazz, name));
  }

  public static Key createKey(Class<?> clazz, String name) {
    return KeyFactory.createKey(clazz.getSimpleName(), name);
  }
}
