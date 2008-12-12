// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

/**
 * @author Max Ross <maxr@google.com>
 */
public final class TestUtils {

  private TestUtils() {}

  public static void assertKeyParentEquals(String parentKey, Entity childEntity, Key childKey) {
    assertEquals(KeyFactory.decodeKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.decodeKey(parentKey), childKey.getParent());
  }

  public static void assertKeyParentEquals(String parentKey, Entity childEntity, String childKey) {
    assertEquals(KeyFactory.decodeKey(parentKey), childEntity.getKey().getParent());
    assertEquals(KeyFactory.decodeKey(parentKey), KeyFactory.decodeKey(childKey).getParent());
  }

  public static void assertKeyParentNull(Entity childEntity, String childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(KeyFactory.decodeKey(childKey).getParent());
  }

  public static void assertKeyParentNull(Entity childEntity, Key childKey) {
    assertNull(childEntity.getKey().getParent());
    assertNull(childKey.getParent());
  }


}
