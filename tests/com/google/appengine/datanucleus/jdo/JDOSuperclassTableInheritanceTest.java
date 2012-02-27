/**********************************************************************
Copyright (c) 2011 Google Inc.

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
package com.google.appengine.datanucleus.jdo;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.test.SuperclassTableInheritanceJDO.*;

import junit.framework.Assert;

import javax.jdo.Extent;
import javax.jdo.JDOFatalUserException;
import javax.jdo.Query;

import java.util.*;

public class JDOSuperclassTableInheritanceTest extends JDOTestCase {
  private final static String PARENT_KIND = "SuperclassTableInheritanceJDO$Parent";
  private final static String PARENTINT_KIND = "SuperclassTableInheritanceJDO$ParentIntDiscriminator";

  public void testCreateAndFindParentAndChildren() throws Exception {
    testCreateAndFindParentAndChildren(TXN_START_END);
  }
  public void testCreateAndFindParentAndChildren_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testCreateAndFindParentAndChildren(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testCreateAndFindParentAndChildren(StartEnd startEnd) throws Exception {
    
    //	create Parent
    Parent p = newParent(startEnd, "Parent");
    Long pId = p.getId();
    Key pKey = KeyFactory.createKey(PARENT_KIND, pId);
    
    //	verify Parent entity
    Entity pe = ds.get(pKey);
    Assert.assertEquals(pKey, pe.getKey());
    Assert.assertEquals("P", pe.getProperty("DISCRIMINATOR"));
    Assert.assertEquals("Parent", pe.getProperty("parentStr"));
    Assert.assertEquals(2, pe.getProperties().size());
    
    //	verify Parent object
    startEnd.start();
    p = pm.getObjectById(Parent.class, pId);
    Assert.assertEquals(pId, p.getId());
    Assert.assertEquals("Parent", p.getParentStr());
    startEnd.end();

    //	create Child11
    Child11 c11 = newChild11(startEnd, "Child11", 11);
    Long c11Id = c11.getId();
    Key c11Key = KeyFactory.createKey(PARENT_KIND, c11Id);
    
    //	verify Child11 entity
    Entity c11e = ds.get(c11Key);
    Assert.assertEquals(c11Key, c11e.getKey());
    Assert.assertEquals("C11", c11e.getProperty("DISCRIMINATOR"));
    Assert.assertEquals("Child11", c11e.getProperty("parentStr"));
    Assert.assertEquals(11, ((Long) c11e.getProperty("child11Integer")).intValue());
    Assert.assertTrue(c11e.hasProperty("child11Manys"));
    Assert.assertNull(c11e.getProperty("child11Manys"));
    Assert.assertEquals(4, c11e.getProperties().size());
    
    //	verify Child11 object
    startEnd.start();
    c11 = pm.getObjectById(Child11.class, c11Id);
    Assert.assertEquals(c11Id, c11.getId());
    Assert.assertEquals("Child11", c11.getParentStr());
    Assert.assertEquals(new Integer(11), c11.getChild11Integer());
    Assert.assertTrue(c11.getChild11Manys().isEmpty());
    startEnd.end();

    //	create Child12
    Child11Many c12m = new Child11Many("Child11ManyStr");
    Child12 c12 = newChild12(startEnd, "Child12", null, 112, null, new Embedded1("Child12Embedded1"), c12m);
    Long c12Id = c12.getId();
    String c12mId = c12m.getId();
    Key c12Key = KeyFactory.createKey(PARENT_KIND, c12Id);
    
    //	verify Child12 entity
    Entity c12e = ds.get(c12Key);
    Assert.assertEquals(c12Key, c12e.getKey());
    Assert.assertEquals("C12", c12e.getProperty("DISCRIMINATOR"));
    Assert.assertEquals("Child12", c12e.getProperty("parentStr"));
    Assert.assertTrue(c12e.hasProperty("child11Integer"));
    Assert.assertNull(c12e.getProperty("child11Integer"));
    Assert.assertEquals(112, ((Long) c12e.getProperty("child12Int")).intValue());
    Assert.assertTrue(c12e.hasProperty("value"));
    Assert.assertEquals(0d, c12e.getProperty("value"));
    Assert.assertEquals("Child12Embedded1", c12e.getProperty("string"));
    Assert.assertTrue(c12e.hasProperty("child11Manys"));
    Assert.assertEquals(1, ((List<Key>)c12e.getProperty("child11Manys")).size());
    Assert.assertEquals(KeyFactory.stringToKey(c12mId), ((List<Key>)c12e.getProperty("child11Manys")).get(0));
    Assert.assertEquals(7, c12e.getProperties().size());
    
    //	verify Child12 object
    startEnd.start();
    c12 = pm.getObjectById(Child12.class, c12Id);
    Assert.assertEquals(c12Id, c12.getId());
    Assert.assertEquals("Child12", c12.getParentStr());
    Assert.assertNull(c12.getChild11Integer());
    Assert.assertEquals(112, c12.getChild12Int());
    Assert.assertNotNull(c12.getEmbedded1());
    Assert.assertEquals("Child12Embedded1", c12.getEmbedded1().getStr());
    Assert.assertNotNull(c12.getChild11Manys());
    Assert.assertEquals(1, c12.getChild11Manys().size());
    Assert.assertEquals("Child11ManyStr", c12.getChild11Manys().get(0).getStr());
    startEnd.end();
    
    //	create Child21
    Child21 c21 = newChild21(startEnd, null, 21L);    
    Long c21Id = c21.getId();
    Key c21Key = KeyFactory.createKey(PARENT_KIND, c21Id);
    
    //	verify Child21 entity
    Entity c21e = ds.get(c21Key);
    Assert.assertEquals(c21Key, c21e.getKey());
    Assert.assertEquals("C21", c21e.getProperty("DISCRIMINATOR"));
    Assert.assertTrue(c21e.hasProperty("parentStr"));
    Assert.assertNull(c21e.getProperty("parentStr"));
    Assert.assertEquals(21L, c21e.getProperty("child21Long"));
    Assert.assertTrue(c21e.hasProperty("str"));
    Assert.assertNull(c21e.getProperty("str"));
    Assert.assertTrue(c21e.hasProperty("dbl"));
    Assert.assertNull(c21e.getProperty("dbl"));
    Assert.assertEquals(5, c21e.getProperties().size());

    //	verify Child21 object
    startEnd.start();
    c21 = pm.getObjectById(Child21.class, c21Id);
    Assert.assertEquals(c21Id, c21.getId());
    Assert.assertEquals(21L, c21.getChild21Long());
    Assert.assertNull(c21.getParentStr());
    startEnd.end();

    //	create Child22
    Child22 c22 = newChild22(startEnd, "ParentChild22", "Child22", Boolean.TRUE, new Embedded2("Embedded2Child22", -7d));
    Long c22Id = c22.getId();
    Key c22Key = KeyFactory.createKey(PARENT_KIND, c22Id);

    //	verify Child22 entity
    Entity c22e = ds.get(c22Key);
    Assert.assertEquals(c22Key, c22e.getKey());    
    Assert.assertEquals("C22", c22e.getProperty("DISCRIMINATOR"));
    Assert.assertEquals("ParentChild22", c22e.getProperty("parentStr"));
    Assert.assertEquals("Child22", c22e.getProperty("child22Str"));
    Assert.assertEquals(0L, c22e.getProperty("child21Long"));
    Assert.assertEquals(Boolean.TRUE, c22e.getProperty("value_0"));
    Assert.assertEquals(new Text("Embedded2Child22"), c22e.getProperty("str"));
    Assert.assertEquals(-7d, c22e.getProperty("dbl"));
    Assert.assertEquals(7, c22e.getProperties().size());

    //	verify Child22 object
    startEnd.start();
    c22 = pm.getObjectById(Child22.class, c22Id);
    Assert.assertEquals(c22Id, c22.getId());
    Assert.assertEquals("ParentChild22", c22.getParentStr());
    Assert.assertEquals(0L, c22.getChild21Long());
    Assert.assertEquals("Child22", c22.getChild22Str());
    startEnd.end();
    
    Assert.assertEquals(5, countForKind(PARENT_KIND));
  }
  
  public void testQueryParent() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("datanucleus.appengine.getExtentCanReturnSubclasses", Boolean.TRUE.toString());
    switchDatasource(PersistenceManagerFactoryName.transactional, props);
    testQueryParent(TXN_START_END);
  }
  public void testQueryParent_NoTxn() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("datanucleus.appengine.getExtentCanReturnSubclasses", Boolean.TRUE.toString());
    switchDatasource(PersistenceManagerFactoryName.nontransactional, props);
    testQueryParent(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testQueryParent(StartEnd startEnd) {
    Parent p = newParent(startEnd, "Parent");
    Long pId = p.getId();

    newChild21(startEnd, null, 21L);
    
    startEnd.start();
    Query q = pm.newQuery("select from " + Parent.class.getName() + " where id == :id");
    List<Parent> r = (List<Parent>)q.execute(pId);
    Assert.assertEquals(1, r.size());
    p = r.get(0);
    Assert.assertEquals(pId, p.getId());
    Assert.assertEquals("Parent", p.getParentStr());
    startEnd.end();
    
    startEnd.start();
    Extent<Parent> e = pm.getExtent(Parent.class, true);
    q = pm.newQuery(e);
    r = (List<Parent>)q.execute();
    Assert.assertEquals(2, r.size());
    startEnd.end();

  }
  public void testUpdateDependent() throws Exception {
    testUpdateDependent(TXN_START_END);
  }
  public void testUpdateDependent_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(PROP_DETACH_ON_CLOSE, true);
    testUpdateDependent(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testUpdateDependent(StartEnd startEnd) throws Exception {
    //	create Child12
    Child12 c12_0 = newChild12(startEnd, null, null, 12, null, new Embedded1("Child12Embedded1(1)"));
    Long c12_0Id = c12_0.getId();
    Key c12_0key = KeyFactory.createKey(PARENT_KIND, c12_0Id);
    
    //	add a Child11Many to Child12
    startEnd.start();
    c12_0 = pm.getObjectById(Child12.class, c12_0Id);
    Assert.assertTrue(c12_0.getChild11Manys().isEmpty());
    Child11Many c12m_0 = new Child11Many("Child12Embedded1(1)/Child11ManyStr(1)");
    c12_0.getChild11Manys().add(c12m_0);
    startEnd.end();
    String c12m_0Id = c12m_0.getId();
    
    // more objects to prove we update and delete the correct one
    Child12 c12_1 = newChild12(startEnd, null, null, 121, null, new Embedded1("Child12Embedded1(2)"));
    Long c12_1Id = c12_1.getId();
    startEnd.start();
    c12_1 = pm.getObjectById(Child12.class, c12_1Id);
    c12_1.getChild11Manys().add(new Child11Many("Child12Embedded1(2)/Child11ManyStr(1)"));
    startEnd.end();
    
    Assert.assertEquals(2, countForKind(PARENT_KIND));
    Assert.assertEquals(2, countForClass(Child11Many.class));
    
    //	check the key of  Child11Many in the Entity
    Entity c12e = ds.get(c12_0key);
    Assert.assertEquals(KeyFactory.stringToKey(c12m_0Id), ((List<Key>)c12e.getProperty("child11Manys")).get(0));
    
    startEnd.start();
    Query q = pm.newQuery();
    q.setIgnoreCache(true);
    q.setClass(Child12.class);
    q.setRange(0, 1);
    List<Child11> r = (List<Child11>)q.execute();
    Assert.assertEquals(1, r.size());
    Child11Many c12m_1 = new Child11Many("Child12Embedded1(1)/Child11ManyStr(2)");
    r.get(0).getChild11Manys().add(c12m_1);
    startEnd.end();
    String c12m_1Id = c12m_1.getId();
    
    Assert.assertEquals(2, countForKind(PARENT_KIND));
    Assert.assertEquals(3, countForClass(Child11Many.class));

    c12e = ds.get(c12_0key);
    Assert.assertEquals(KeyFactory.stringToKey(c12m_0Id), ((List<Key>)c12e.getProperty("child11Manys")).get(0));
    Assert.assertEquals(KeyFactory.stringToKey(c12m_1Id), ((List<Key>)c12e.getProperty("child11Manys")).get(1));
    
    startEnd.start();
    c12_0 = (Child12)pm.getObjectById(Child12.class, c12_0Id);
    c12_0.getChild11Manys().remove(0);
    startEnd.end();

    c12e = ds.get(c12_0key);
    Assert.assertEquals(1, ((List<Key>)c12e.getProperty("child11Manys")).size());
    Assert.assertEquals(KeyFactory.stringToKey(c12m_1Id), ((List<Key>)c12e.getProperty("child11Manys")).get(0));
    
    Assert.assertEquals(2, countForKind(PARENT_KIND));
    Assert.assertEquals(2, countForClass(Child11Many.class));
    
    startEnd.start();
    pm.deletePersistent(pm.getObjectById(Child12.class, c12_0Id));
    startEnd.end();

    Assert.assertEquals(1, countForClass(Child11Many.class));
    Assert.assertEquals(1, countForKind(PARENT_KIND));
  }
  
  public void testQueryChildren() {
    testQueryChildren(TXN_START_END);
  }
  public void testQueryChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testQueryChildren(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testQueryChildren(StartEnd startEnd) {
    Child11 c11_0 = newChild11(startEnd, "Child11", 11);
    Long c11Id = c11_0.getId();
    
    Child12 c12_0 = newChild12(startEnd, "Child12", 111, 112, null, new Embedded1("Child12Embedded1"));
    Long c12_0Id = c12_0.getId();

    newChild12(startEnd, "Child12", 111, 112, null, null);

    startEnd.start();
    Query q = pm.newQuery("select from " + Parent.class.getName() + " where id == :id");
    List<Child11> r11 = (List<Child11>)q.execute(c11Id);
    Assert.assertEquals(1, r11.size());
    c11_0 = r11.get(0);
    Assert.assertEquals(c11Id, c11_0.getId());
    Assert.assertEquals("Child11", c11_0.getParentStr());
    Assert.assertEquals(new Integer(11), c11_0.getChild11Integer());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child11.class.getName() + " where child12Int > 0");
    q.addExtension(DatastoreManager.QUERYEXT_INMEMORY_WHEN_UNSUPPORTED, "false");
    try {
      q.execute();
      fail("expected JDOFatalUserException");
    } catch (JDOFatalUserException ex) {
      // good Child11 does not have a property child12Int
    } finally {
      startEnd.end();
    }

    startEnd.start();
    q = pm.newQuery("select from " + Child12.class.getName() + " where embedded1.str == :str");
    List<Child12> r12 = (List<Child12>)q.execute("Child12Embedded1");
    Assert.assertEquals(1, r12.size());
    c12_0 = r12.get(0);
    Assert.assertEquals(c12_0Id, c12_0.getId());
    Assert.assertEquals("Child12Embedded1", c12_0.getEmbedded1().getStr());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child11.class.getName() + " where parentStr == :str");
    r11 = (List<Child11>)q.execute("Child12");
    Assert.assertEquals(2, r11.size());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child12.class.getName() + " where parentStr == :parentStr && embedded1.str == :str");
    r12 = (List<Child12>)q.execute("Child12", "Child12Embedded1");
    Assert.assertEquals(1, r12.size());
    startEnd.end();
  }
  
  public void testQueryParentAndChildren() {
    testQueryParentAndChildren(TXN_START_END);
  }
  public void testQueryParentAndChildren_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testQueryParentAndChildren(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testQueryParentAndChildren(StartEnd startEnd) {
    Child11 c11 = newChild11(startEnd, "A", 111);
    Long c11Id = c11.getId();

    Child12 c12 = newChild12(startEnd, "B", 112, 211, 8.15f, null);
    Long c12Id = c12.getId();
    
    Parent p = newParent(startEnd, "C");
    Long pId = p.getId();

    Child21 c21 = newChild21(startEnd, "D", 121L);
    Long c21Id = c21.getId();

    Child22 c22 = newChild22(startEnd, "E", "Child22", Boolean.TRUE, null);
    Long c22Id = c22.getId();
    
    Assert.assertEquals(5, countForKind(PARENT_KIND));
    
    startEnd.start();
    Query q = pm.newQuery("select from " + Parent.class.getName() + " order by parentStr desc");
    q.setIgnoreCache(true);
    List<Parent> r = (List<Parent>)q.execute();
    Assert.assertEquals(5, r.size());

    c22 = (Child22)r.get(0);
    Assert.assertEquals(c22Id, c22.getId());
    Assert.assertEquals("E", c22.getParentStr());
    Assert.assertEquals("Child22", c22.getChild22Str());
    Assert.assertEquals(Boolean.TRUE, c22.getValue());

    c21 = (Child21)r.get(1);
    Assert.assertEquals(c21Id, c21.getId());
    Assert.assertEquals("D", c21.getParentStr());
    Assert.assertEquals(121L, c21.getChild21Long());

    p = r.get(2);
    Assert.assertEquals(pId, p.getId());
    Assert.assertEquals("C", p.getParentStr());
    
    c12 = (Child12)r.get(3);
    Assert.assertEquals(c12Id, c12.getId());
    Assert.assertEquals("B", c12.getParentStr());
    Assert.assertEquals(new Integer(112), c12.getChild11Integer());
    Assert.assertEquals(211, c12.getChild12Int());
    Assert.assertEquals(8.15f, c12.getValue());

    c11 = (Child11)r.get(4);
    Assert.assertEquals(c11Id, c11.getId());
    Assert.assertEquals("A", c11.getParentStr());
    Assert.assertEquals(new Integer(111), c11.getChild11Integer());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child11.class.getName() + " where child11Integer >= 111");
    r = (List<Parent>)q.execute();
    Assert.assertEquals(2, r.size());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child12.class.getName() + " where value >= 8.0");
    r = (List<Parent>)q.execute();
    Assert.assertEquals(1, r.size());
    Assert.assertEquals(c12Id, r.get(0).getId());
    startEnd.end();
    
    startEnd.start();
    q = pm.newQuery("select from " + Child22.class.getName() + " where value == true");
    r = (List<Parent>)q.execute();
    Assert.assertEquals(1, r.size());
    Assert.assertEquals(c22Id, r.get(0).getId());
    startEnd.end();
  }

  public void testQueryHierarchy()throws Exception {
    testQueryHierarchy(TXN_START_END);
  }
  public void testQueryHierarchy_NoTxn() throws Exception{
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    testQueryHierarchy(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testQueryHierarchy(StartEnd startEnd) throws Exception {
    
    Child11 c11 = newChild11(startEnd, "A", 111);
    Long c11Id = c11.getId();

    Child12 c12 = newChild12(startEnd, "A", 211, 112, 8.15f, null);
    Long c12Id = c12.getId();

    Parent p = newParent(startEnd, "A");
    Long pId = p.getId();

    c12 = newChild12(startEnd, "B", 2211, 2112, 47.11f, null);

    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    
    Query q = pm.newQuery("select from " + Parent.class.getName() + " where parentStr == 'A'");
    q.setIgnoreCache(true);
    List<Parent> r = (List<Parent>)q.execute();
    Assert.assertEquals(3, r.size());
    Set<Long> ids = new HashSet<Long>(Arrays.asList(new Long[] {c11Id, c12Id, pId}));
    for(Parent rp : r) {
      Assert.assertTrue(ids.remove(rp.getId()));
    }
    
    q = pm.newQuery("select from " + Child11.class.getName() + " where parentStr == 'A' && child11Integer > 110");
    r = (List<Parent>)q.execute();
    Assert.assertEquals(2, r.size());
    ids = new HashSet<Long>(Arrays.asList(new Long[] {c11Id, c12Id}));
    for(Parent rp : r) {
      Assert.assertTrue(ids.remove(rp.getId()));
    }

    q = pm.newQuery("select from " + Child12.class.getName() + " where parentStr == 'A'");
    r = (List<Parent>)q.execute();
    Assert.assertEquals(1, r.size());
    Assert.assertEquals(c12Id, r.get(0).getId());
  }

  public void testUpdateAndDeleteParentAndChilds() throws Exception {
    testUpdateAndDeleteParentAndChilds(TXN_START_END);
  }
  public void testUpdateAndDeleteParentAndChilds_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(PROP_DETACH_ON_CLOSE, true);
    testUpdateAndDeleteParentAndChilds(NEW_PM_START_END);
  }
  private void testUpdateAndDeleteParentAndChilds(StartEnd startEnd) throws Exception {
    Child11 c11 = newChild11(startEnd, "Parent", 211);
    Long c11Id = c11.getId();
    Key c11Key = KeyFactory.createKey(PARENT_KIND, c11.getId());
    
    startEnd.start();
    c11 = (Child11) pm.getObjectById(Child11.class, c11Id);
    Assert.assertEquals(c11Id, c11.getId());
    Assert.assertEquals("Parent", c11.getParentStr());
    c11.setParentStr("Child22");
    startEnd.end();
    
    verifyDiscriminator(c11Id, "C11");
    Entity c11e = ds.get(c11Key);
    Assert.assertEquals("Child22", c11e.getProperty("parentStr"));
    
    startEnd.start();
    c11 = (Child11) pm.getObjectById(Child11.class, c11Id);
    Assert.assertEquals("Child22", c11.getParentStr());
    Assert.assertEquals(new Integer(211), c11.getChild11Integer());
    startEnd.end();

    Child12 c12 = newChild12(startEnd, null, null, 0, null, null);
    Long c12Id = c12.getId();

    Assert.assertEquals(2, countForKind(PARENT_KIND));

    startEnd.start();
    pm.deletePersistent(pm.getObjectById(Child11.class, c11Id));
    startEnd.end();
    
    Assert.assertEquals(1, countForKind(PARENT_KIND));
    
    startEnd.start();
    c12 = (Child12) pm.getObjectById(Child12.class, c12Id);
    Assert.assertEquals(c12Id, c12.getId());
    
    startEnd.end();
  }
  
  public void testCreateIntDiscriminator() throws Exception {
    testCreateIntDiscriminator(TXN_START_END);
  }
  public void testCreateIntDiscriminator_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(PROP_DETACH_ON_CLOSE, true);
    testCreateIntDiscriminator(NEW_PM_START_END);
  }
  private void testCreateIntDiscriminator(StartEnd startEnd) throws Exception {
    ParentIntDiscriminator p = new ParentIntDiscriminator();
    p.setParentStr("ParentInt");
    makePersistentInTxn(p, startEnd);
    
    String pId = p.getId();
    Key pKey = KeyFactory.stringToKey(pId);
    Entity pe = ds.get(pKey);
    
    Assert.assertEquals(pKey, pe.getKey());
    Assert.assertEquals(1L, pe.getProperty("type"));  
    Assert.assertEquals("ParentInt", pe.getProperty("parentStr"));
    Assert.assertEquals(2, pe.getProperties().size());
    
    long now = System.currentTimeMillis();
    ChildDateIntDiscriminator cd = new ChildDateIntDiscriminator();
    cd.setParentStr("ChildDateInt");
    cd.setValue(new Date(now));
    makePersistentInTxn(cd, startEnd);
    
    String cdId = cd.getId();
    Key cdKey = KeyFactory.stringToKey(cdId);
    Entity cde = ds.get(cdKey);
    
    Assert.assertEquals(cdKey, cde.getKey());
    Assert.assertEquals(2L, cde.getProperty("type"));  
    Assert.assertEquals("ChildDateInt", cde.getProperty("parentStr"));
    Assert.assertEquals(now, ((Date)cde.getProperty("date")).getTime());  
    Assert.assertEquals(3, cde.getProperties().size());
    
    ChildBoolIntDiscriminator cb = new ChildBoolIntDiscriminator();
    cb.setParentStr("ChildBoolInt");
    cb.setValue(Boolean.TRUE);
    makePersistentInTxn(cb, startEnd);
    
    String cbId = cb.getId();
    Key cbKey = KeyFactory.stringToKey(cbId);
    Entity cbe = ds.get(cbKey);
    
    Assert.assertEquals(cbKey, cbe.getKey());
    Assert.assertEquals(3L, cbe.getProperty("type"));  
    Assert.assertEquals("ChildBoolInt", cbe.getProperty("parentStr"));
    Assert.assertEquals(Boolean.TRUE, cbe.getProperty("bool"));  
    Assert.assertEquals(3, cbe.getProperties().size());
    
    Assert.assertEquals(3, countForKind(PARENTINT_KIND));
  }
  
  public void testQueryIntDiscriminator() throws Exception {
    testQueryIntDiscriminator(TXN_START_END);
  }
  public void testQueryIntDiscriminator_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(PROP_DETACH_ON_CLOSE, true);
    testQueryIntDiscriminator(NEW_PM_START_END);
  }
  @SuppressWarnings("unchecked")
  private void testQueryIntDiscriminator(StartEnd startEnd) throws Exception {
    ChildDateIntDiscriminator cd = new ChildDateIntDiscriminator();
    cd.setParentStr("A");
    cd.setValue(new Date());
    makePersistentInTxn(cd, startEnd);
    String cdId = cd.getId();

    ChildBoolIntDiscriminator cb = new ChildBoolIntDiscriminator();
    cb.setParentStr("C");
    cb.setValue(Boolean.TRUE);
    makePersistentInTxn(cb, startEnd);
    String cbId = cb.getId();
    
    ParentIntDiscriminator p = new ParentIntDiscriminator();
    p.setParentStr("B");
    makePersistentInTxn(p, startEnd);
    String pId = p.getId();

    startEnd.start();
    Query q = pm.newQuery("select from " + ParentIntDiscriminator.class.getName() + " order by parentStr");
    List<ParentIntDiscriminator> r = (List<ParentIntDiscriminator>)q.execute();
    Assert.assertEquals(3, r.size());
    
    Assert.assertEquals(cdId, r.get(0).getId());
    Assert.assertTrue(r.get(0) instanceof ChildDateIntDiscriminator);
    Assert.assertEquals(pId, r.get(1).getId());
    Assert.assertTrue(r.get(1) instanceof ParentIntDiscriminator);
    Assert.assertEquals(cbId, r.get(2).getId());
    Assert.assertTrue(r.get(2) instanceof ChildBoolIntDiscriminator);
    startEnd.end();
  }
  
  public void testUpdateIntDiscriminator() throws Exception {
    testUpdateIntDiscriminator(TXN_START_END);
  }
  public void testUpdateIntDiscriminator_NoTxn() throws Exception {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    getExecutionContext().setProperty(PROP_DETACH_ON_CLOSE, true);
    testUpdateIntDiscriminator(NEW_PM_START_END);
  }
  public void testUpdateIntDiscriminator(StartEnd startEnd) throws Exception {
    ParentIntDiscriminator p = new ParentIntDiscriminator();
    p.setParentStr("ParentInt");
    makePersistentInTxn(p, startEnd);
    
    String pId = p.getId();
    Key pKey = KeyFactory.stringToKey(pId);
    Assert.assertEquals(PARENTINT_KIND, pKey.getKind());
    
    Entity pe = ds.get(pKey);
    Assert.assertEquals(pKey, pe.getKey());
    Assert.assertEquals(1L, pe.getProperty("type"));  
    Assert.assertEquals("ParentInt", pe.getProperty("parentStr"));
    Assert.assertEquals(2, pe.getProperties().size());

    startEnd.start();
    p = pm.getObjectById(ParentIntDiscriminator.class, pId);
    p.setParentStr("ParentInt(2)");
    startEnd.end();
    
    pe = ds.get(pKey);
    Assert.assertEquals(pKey, pe.getKey());
    Assert.assertEquals(1L, pe.getProperty("type"));  
    Assert.assertEquals("ParentInt(2)", pe.getProperty("parentStr"));
    Assert.assertEquals(2, pe.getProperties().size());
    
    startEnd.start();
    p = pm.getObjectById(ParentIntDiscriminator.class, pId);
    Assert.assertEquals("ParentInt(2)", p.getParentStr());
    startEnd.end();
    
    Assert.assertEquals(1, countForKind(PARENTINT_KIND));
    
    startEnd.start();
    pm.deletePersistent(pm.getObjectById(ParentIntDiscriminator.class, pId));
    startEnd.end();

    Assert.assertEquals(0, countForKind(PARENTINT_KIND));
  }
  
  public void testMissingDiscriminator() {
    beginTxn();
    try {
      pm.makePersistent(new ChildToParentWithoutDiscriminator());
      commitTxn();
      fail("expected exeception");
    } catch (Exception e) {
      // good
    }
  }

  public void testMissingDiscriminator_NoTxn() {
    switchDatasource(PersistenceManagerFactoryName.nontransactional);
    try {
      pm.makePersistent(new ChildToParentWithoutDiscriminator());
      pm.close();
      fail("expected exeception");
    } catch (Exception e) {
      // good
    }
  }

  private Parent newParent(StartEnd startEnd, String parentStr) {
    Parent p = new Parent();
    p.setParentStr(parentStr);
    makePersistentInTxn(p, startEnd);
    return p;
  }
  
  private Child11 newChild11(StartEnd startEnd, String parentStr, int child11Int) {
    Child11 c11 = new Child11();
    c11.setParentStr(parentStr);
    c11.setChild11Integer(child11Int);
    makePersistentInTxn(c11, startEnd);
    return c11;
  }
  
  private Child12 newChild12(StartEnd startEnd, String parentStr, Integer child11Integer, int child12Int, Float value, Embedded1 embedded1, Child11Many... child11Many) {
    Child12 c12 = new Child12();
    c12.setParentStr(parentStr);
    c12.setChild11Integer(child11Integer);
    c12.setChild12Int(child12Int);
    c12.setEmbedded1(embedded1);
    if (value != null) {
      c12.setValue(value);
    }
    if (child11Many != null) {
      for (Child11Many c : child11Many) {
        c12.getChild11Manys().add(c);
      }
    }
    makePersistentInTxn(c12, startEnd);
    return c12;
  }
  
  private Child21 newChild21(StartEnd startEnd, String parentStr, Long child21Long) {
    Child21 c21 = new Child21();
    c21.setParentStr(parentStr);
    c21.setChild21Long(child21Long);
    makePersistentInTxn(c21, startEnd);
    return c21;
  }
  
  private Child22 newChild22(StartEnd startEnd, String parentStr, String child22Str, Boolean value, Embedded2 embedded2) {
    Child22 c22 = new Child22();
    c22.setParentStr(parentStr);
    c22.setChild22Str("Child22");
    c22.setEmbedded2(embedded2);
    c22.setValue(value);
    makePersistentInTxn(c22, startEnd);
    return c22;
  }
  
  private void verifyDiscriminator(Long id, String expectedDiscriminator) throws Exception {
    Entity entity = ds.get(KeyFactory.createKey(PARENT_KIND, id));
    Assert.assertEquals(expectedDiscriminator, entity.getProperty("DISCRIMINATOR"));
  }

  @SuppressWarnings("deprecation")
  private int countForKind(String kind) {
    return ds.prepare(
        new com.google.appengine.api.datastore.Query(kind)).countEntities();
  }
}