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

  public static void assertKeyParentNull(Entity childEntity, String childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(KeyFactory.stringToKey(childKey).getParent());
  }

  public static void assertKeyParentNull(Entity childEntity, Key childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(childKey.getParent());
  }


}
