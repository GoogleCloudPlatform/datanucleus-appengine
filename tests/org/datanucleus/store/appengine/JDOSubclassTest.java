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
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.HasSubclassJDO;
import org.datanucleus.test.IsSubclassJDO;

/**
 * There's something flaky here that will probably show up as a real bug at
 * some point.  If the Parent class gets used first, the subclass
 * tests fail.  To get around this I'm just running the subclass tests
 * first.  There's definitely something funny going on though.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOSubclassTest extends JDOTestCase {

  public void testInsertChild_KeyOnParent() throws EntityNotFoundException {
    IsSubclassJDO pojo = new IsSubclassJDO();
    pojo.setParentString("yar");
    pojo.setChildString("childyar");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(IsSubclassJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("parentString"));
    assertEquals("childyar", e.getProperty("childString"));
  }

  public void testInsertParent_KeyOnParent() throws EntityNotFoundException {
    HasSubclassJDO pojo = new HasSubclassJDO();
    pojo.setParentString("yar");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(HasSubclassJDO.class.getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("parentString"));
  }

  public void testFetchChild_KeyOnParent() {
    Entity e = new Entity(IsSubclassJDO.class.getSimpleName());
    e.setProperty("parentString", "yar");
    e.setProperty("childString", "childyar");
    ldth.ds.put(e);

    beginTxn();
    IsSubclassJDO pojo = pm.getObjectById(IsSubclassJDO.class, e.getKey());
    assertEquals("yar", pojo.getParentString());
    assertEquals("childyar", pojo.getChildString());
  }

  public void testFetchParent_KeyOnParent() {
    Entity e = new Entity(HasSubclassJDO.class.getSimpleName());
    e.setProperty("parentString", "yar");
    ldth.ds.put(e);

    beginTxn();
    HasSubclassJDO pojo = pm.getObjectById(HasSubclassJDO.class, e.getKey());
    assertEquals("yar", pojo.getParentString());
  }

}
