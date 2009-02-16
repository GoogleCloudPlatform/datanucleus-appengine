// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;

import org.datanucleus.test.HasMultiValuePropsJDO;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOMakeTransientTest extends JDOTestCase {

  public void testListAccessibleOutsideTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    Entity e = new Entity(HasMultiValuePropsJDO.class.getSimpleName());
    e.setProperty("strList", Utils.newArrayList("a", "b", "c"));
    e.setProperty("str", "yar");
    ldth.ds.put(e);

    beginTxn();
    HasMultiValuePropsJDO pojo = pm.getObjectById(HasMultiValuePropsJDO.class, e.getKey().getId());
    pojo.setStr("yip");
    pojo.getStrList();
    commitTxn();
    assertEquals("yip", pojo.getStr());
    assertEquals(3, pojo.getStrList().size());
    pm = pmf.getPersistenceManager();
  }
}