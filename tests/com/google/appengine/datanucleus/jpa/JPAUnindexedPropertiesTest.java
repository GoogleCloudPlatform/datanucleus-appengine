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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.test.HasUnindexedPropertiesJPA;


import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAUnindexedPropertiesTest extends JPATestCase {

  private static final Field PROPERTY_MAP_FIELD;
  static {
    try {
      PROPERTY_MAP_FIELD = Entity.class.getDeclaredField("propertyMap");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    PROPERTY_MAP_FIELD.setAccessible(true);
  }

  private static Object getRawProperty(Entity e, String propertyName) throws
                                                                      IllegalAccessException {
    Map<String, Object> rawProps = (Map<String, Object>) PROPERTY_MAP_FIELD.get(e);
    return rawProps.get(propertyName);
  }

  private static boolean isUnindexedProperty(Object obj) {
    return "com.google.appengine.api.datastore.Entity$UnindexedValue".equals(obj.getClass().getName());
  }

  public void testInsert() throws EntityNotFoundException, IllegalAccessException {
    HasUnindexedPropertiesJPA pojo = new HasUnindexedPropertiesJPA();
    pojo.setUnindexedString("str");
    pojo.setUnindexedList(Utils.newArrayList("a", "b", "c"));
    pojo.setUnindexedText(new Text("unindexed text"));
    pojo.setUnindexedBlob(new Blob("unindexed blob".getBytes()));
    pojo.setText(new Text("text"));
    pojo.setBlob(new Blob("blob".getBytes()));

    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity e = ds.get(KeyFactory.createKey(pojo.getClass().getSimpleName(), pojo.getId()));
    assertTrue(isUnindexedProperty(getRawProperty(e, "unindexedString")));
    assertTrue(isUnindexedProperty(getRawProperty(e, "unindexedList")));
    assertTrue(isUnindexedProperty(getRawProperty(e, "unindexedText")));
    assertTrue(isUnindexedProperty(getRawProperty(e, "unindexedBlob")));
    assertTrue(isUnindexedProperty(getRawProperty(e, "text")));
    assertTrue(isUnindexedProperty(getRawProperty(e, "blob")));
  }
}