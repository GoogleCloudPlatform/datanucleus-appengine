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
    ds.put(e);

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