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
