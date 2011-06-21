/*
 * /**********************************************************************
 * Copyright (c) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * **********************************************************************/

package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Link;
import com.google.appengine.datanucleus.test.HasFetchGroupsJDO;


import javax.jdo.Query;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOFetchGroupTest extends JDOTestCase {

  public void testDefaultFetchGroup() {
    HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
    pojo.setStr1("1");
    pojo.setStr2("2");
    pojo.setStr3("3");
    pojo.setStr4("4");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    pm.close();
    pm = pmf.getPersistenceManager();
    beginTxn();
    pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
    commitTxn();
    assertEquals("1", pojo.getStr1());
    assertEquals("2", pojo.getStr2());
    assertNull(pojo.getStr3());
    assertEquals("4", pojo.getStr4());
    beginTxn();
    pojo = pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId());
    pojo.getStr3();
    pojo = pm.detachCopy(pojo);
    commitTxn();
    beginTxn();
    pojo = pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId());
    assertEquals("1", pojo.getStr1());
    assertEquals("2", pojo.getStr2());
    assertEquals("3", pojo.getStr3());
    assertEquals("4", pojo.getStr4());
    commitTxn();
  }

  public void testCustomFetchGroup_ReplaceDefault() {
    HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
    pojo.setStr1("1");
    pojo.setStr2("2");
    pojo.setStr3("3");
    pojo.setStr4("4");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    pm.getFetchPlan().setGroup("fg1");
    pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
    commitTxn();
    assertNull(pojo.getStr1());
    assertNull(pojo.getStr2());
    assertEquals("3", pojo.getStr3());
    assertNull(pojo.getStr4());
  }

  public void testCustomFetchGroup_AddToDefault() {
    HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
    pojo.setStr1("1");
    pojo.setStr2("2");
    pojo.setStr3("3");
    pojo.setStr4("4");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    pm.getFetchPlan().addGroup("fg1");
    pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
    commitTxn();
    assertEquals("1", pojo.getStr1());
    assertEquals("2", pojo.getStr2());
    assertEquals("3", pojo.getStr3());
    assertEquals("4", pojo.getStr4());
  }

  public void testFetchGroupWithQuery() {
    HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
    pojo.setStr1("1");
    pojo.setStr2("2");
    pojo.setStr3("3");
    pojo.setStr4("4");
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();
    beginTxn();
    Query q = pm.newQuery(HasFetchGroupsJDO.class);
    q.setUnique(true);
    pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute());
    commitTxn();
    assertEquals("1", pojo.getStr1());
    assertEquals("2", pojo.getStr2());
    assertNull(pojo.getStr3());
    assertEquals("4", pojo.getStr4());

    beginTxn();
    pm.getFetchPlan().addGroup("fg1");
    pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute());
    commitTxn();
    assertEquals("1", pojo.getStr1());
    assertEquals("2", pojo.getStr2());
    assertEquals("3", pojo.getStr3());
    assertEquals("4", pojo.getStr4());

    beginTxn();
    q = pm.newQuery(HasFetchGroupsJDO.class);
    q.setUnique(true);
    pm.getFetchPlan().setGroup("fg1");
    pojo = (HasFetchGroupsJDO) pm.detachCopy(q.execute());
    commitTxn();
    assertNull(pojo.getStr1());
    assertNull(pojo.getStr2());
    assertEquals("3", pojo.getStr3());
    assertNull(pojo.getStr4());
  }

  public void testFetchGroupOverridesCanBeManuallyUndone() {    
    HasFetchGroupsJDO pojo = new HasFetchGroupsJDO();
    pojo.setLink(new Link("blarg"));
    makePersistentInTxn(pojo, TXN_START_END);
    beginTxn();
    pojo = pm.detachCopy(pm.getObjectById(HasFetchGroupsJDO.class, pojo.getId()));
    commitTxn();
    assertNull(pojo.getLink());
  }
}