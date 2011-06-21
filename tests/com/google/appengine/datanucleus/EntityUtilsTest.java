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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;

/**
 * @author Max Ross <maxr@google.com>
 */
public class EntityUtilsTest extends DatastoreTestCase {

//  private DatastoreTestHelper dth;
//  @Override
//  protected void setUp() throws Exception {
//    super.setUp();
//    dth = new DatastoreTestHelper();
//    dth.setUp();
//  }
//
//  @Override
//  protected void tearDown() throws Exception {
//    dth.tearDown(false);
//    dth = null;
//    super.tearDown();
//  }
//
  public void testUnencodedStringToEncodedString() {
    String keyStr = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(true, String.class), Object.class, "yar", null, false);
    assertEquals(KeyFactory.createKey("Object", "yar"), KeyFactory.stringToKey(keyStr));
  }

  public void testUnencodedStringToUnencodedString() {
    String keyStr = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class, "yar", null, false);
    assertEquals("yar", keyStr);

  }

  public void testUnencodedStringToLong() {
    try {
    EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class, "yar", null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testUnencodedStringToKey() {
    Key key = (Key) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class, "yar", null, false);
    assertEquals(KeyFactory.createKey("Object", "yar"), key);
  }

  public void testEncodedStringToEncodedString() {
    Key key = KeyFactory.createKey("Object", "yar");
    String keyStr = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(true, String.class), Object.class,
        KeyFactory.keyToString(key), null, false);
    assertEquals(key, KeyFactory.stringToKey(keyStr));
  }

  public void testEncodedStringToUnencodedString() {
    Key key = KeyFactory.createKey("Object", "yar");
    String name = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class,
        KeyFactory.keyToString(key), null, false);
    assertEquals("yar", name);
  }

  public void testEncodedStringToUnencodedString_IdSet() {
    Key key = KeyFactory.createKey("Object", 44);
    try {
      EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class,
        KeyFactory.keyToString(key), null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testEncodedStringToLong() {
    Key key = KeyFactory.createKey("Object", 44);
    long id = (Long) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class,
        KeyFactory.keyToString(key), null, false);
    assertEquals(44, id);
  }

  public void testEncodedStringToLong_NameSet() {
    Key key = KeyFactory.createKey("Object", "yar");
    try {
      EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class,
        KeyFactory.keyToString(key), null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testEncodedStringToKey_NameSet() {
    Key input = KeyFactory.createKey("Object", "yar");
    Key output = (Key) EntityUtils.idToInternalKey(
      "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class,
      KeyFactory.keyToString(input), null, false);
    assertEquals(input, output);
  }

  public void testEncodedStringToKey_IdSet() {
    Key input = KeyFactory.createKey("Object", 33);
    Key output = (Key) EntityUtils.idToInternalKey(
      "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class,
      KeyFactory.keyToString(input), null, false);
    assertEquals(input, output);
  }

  public void testLongToEncodedString() {
    String output = (String) EntityUtils.idToInternalKey(
      "Object", new PrimaryKeyMemberMetaData(true, String.class), Object.class, 44, null, false);
    assertEquals(KeyFactory.createKey("Object", 44), KeyFactory.stringToKey(output));
  }

  public void testLongToUnencodedString() {
    try {
      EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class, 44, null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testLongToLong() {
    long output = (Long) EntityUtils.idToInternalKey(
      "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class, 44, null, false);
    assertEquals(44, output);
  }

  public void testLongToKey() {
    Key output = (Key) EntityUtils.idToInternalKey(
      "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class, 44, null, false);
    assertEquals(KeyFactory.createKey("Object", 44), output);

  }

  public void testKeyToEncodedString() {
    Key key = KeyFactory.createKey("Object", 44);
    String output = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(true, String.class), Object.class,
        key, null, false);
    assertEquals(key, KeyFactory.stringToKey(output));
  }

  public void testKeyToUnencodedString_NameSet() {
    Key key = KeyFactory.createKey("Object", "yar");
    String output = (String) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class,
        key, null, false);
    assertEquals("yar", output);
  }

  public void testKeyToUnencodedString_IdSet() {
    Key key = KeyFactory.createKey("Object", 44);
    try {
      EntityUtils.idToInternalKey(
          "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class, key, null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testKeyToLong_NameSet() {
    Key key = KeyFactory.createKey("Object", "yar");
    try {
      EntityUtils.idToInternalKey(
          "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class, key, null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testKeyToKey() {
    Key key = KeyFactory.createKey("Object", "yar");
    Key output = (Key) EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class, key, null, false);
    assertEquals(key, output);
  }

  public void testNullToEncodedString() {
    Object output = EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(true, String.class), Object.class, null, null, false);
    assertNull(output);
  }

  public void testNullToUnencodedString() {
    Object output = EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, String.class), Object.class, null, null, false);
    assertNull(output);
  }

  public void testNullToLong() {
    Object output = EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Long.class), Object.class, null, null, false);
    assertNull(output);
  }

  public void testNullToKey() {
    Object output = EntityUtils.idToInternalKey(
        "Object", new PrimaryKeyMemberMetaData(false, Key.class), Object.class, null, null, false);
    assertNull(output);
  }

  public void testEncodedStringOfWrongKind() {
    Key key = KeyFactory.createKey("Object", "yar");
    try {
      EntityUtils.idToInternalKey(
          "NotObject", new PrimaryKeyMemberMetaData(true, String.class), Object.class,
          KeyFactory.keyToString(key), null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  public void testKeyOfWrongKind() {
    Key key = KeyFactory.createKey("Object", "yar");
    try {
      EntityUtils.idToInternalKey(
          "NotObject", new PrimaryKeyMemberMetaData(true, Key.class), Object.class, key, null, false);
      fail("expected exception");
    } catch (NucleusUserException e) {
      // good
    }
  }

  private static final class PrimaryKeyMemberMetaData extends AbstractMemberMetaData {

    private final boolean hasEncodedPkExtension;
    private final Class<?> type;

    private PrimaryKeyMemberMetaData(boolean hasEncodedPkExtension, Class<?> type) {
      super(null, "Jimmy");
      this.hasEncodedPkExtension = hasEncodedPkExtension;
      this.type = type;
    }

    @Override
    public Class getType() {
      return type;
    }

    @Override
    public boolean hasExtension(String key) {
      return DatastoreManager.ENCODED_PK.equals(key) && hasEncodedPkExtension;
    }
  }
}
