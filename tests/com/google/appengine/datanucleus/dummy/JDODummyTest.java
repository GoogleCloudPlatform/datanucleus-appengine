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
package com.google.appengine.datanucleus.dummy;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import com.google.appengine.datanucleus.jdo.JDOTestCase;
import com.google.appengine.datanucleus.test.Flight;
import com.google.appengine.datanucleus.test.JDOMockBasicObject;

/**
 */
public class JDODummyTest extends JDOTestCase {

  /*public void testInsertAndQuery() {
    pm.setProperty("datanucleus.detachOnClose", "true");

    pm.currentTransaction().begin();
    Flight fl1 = new Flight("LGW", "CHI", "BA101", 101, 202);
    pm.makePersistent(fl1);
    pm.currentTransaction().commit();

    pm.currentTransaction().begin();
    Flight fl2 = new Flight("MAD", "LHR", "IB134", 101, 201);
    pm.makePersistent(fl2);
    pm.currentTransaction().commit();

    pm.currentTransaction().begin();
    Query q = pm.newQuery(Flight.class);
    List<Flight> flights = (List<Flight>) q.execute();
    pm.currentTransaction().commit();

    pm.close();

    Iterator<Flight> iter = flights.iterator();
    while (iter.hasNext())
    {
        Flight fl = iter.next();
        NucleusLogger.GENERAL.info(">> fl origin=" + fl.getOrigin() + " dest=" + fl.getDest() + " state=" + JDOHelper.getObjectState(fl));
    }
  }*/
  public void testPersistThenDelete() { 
    Properties newProperties = new Properties(); 
    newProperties.put("javax.jdo.PersistenceManagerFactoryClass", "com.google.appengine.datanucleus.jdo.DatastoreJDOPersistenceManagerFactory" ); 
    newProperties.put("javax.jdo.option.ConnectionURL", "appengine");
    newProperties.put("javax.jdo.option.RetainValues", "true"); 
    newProperties.put("datanucleus.appengine.autoCreateDatastoreTxns", "true"); 
    newProperties.put("datanucleus.appengine.autoCreateDatastoreTxns", "true"); 
    PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(newProperties); 
    PersistenceManager pm = pmf.getPersistenceManager();

    NucleusLogger.GENERAL.info(">> testPersistThenDelete doing persist");
    JDOMockBasicObject m = new JDOMockBasicObject();
    pm.makePersistent(m);
    NucleusLogger.GENERAL.info(">> testPersistThenDelete after persist");
    long id = m.getId();
    assertTrue(id > 0); 
        JDOMockBasicObject k = (JDOMockBasicObject) pm.getObjectById( 
                        JDOMockBasicObject.class, id); 
    pm.deletePersistent(k);
    NucleusLogger.GENERAL.info(">> testPersistThenDelete after delete k.state=" + JDOHelper.getObjectState(k));

    Query query = pm.newQuery(JDOMockBasicObject.class); 
    query.setFilter("key == k"); 
    query.declareParameters("com.google.appengine.api.datastore.Key k");
    NucleusLogger.GENERAL.info(">> testPersistThenDelete doing query");
    long number = query.deletePersistentAll(k.getKey());
    NucleusLogger.GENERAL.info(">> testPersistThenDelete num.deleted=" + number);
    try {
      NucleusLogger.GENERAL.info(">> getObjectById(JDOMockBasicObject, id=" + id + ")");
      JDOMockBasicObject l = (JDOMockBasicObject) pm.getObjectById(JDOMockBasicObject.class, id); 
      fail("not supposed to get here l.state=" + JDOHelper.getObjectState(l));
    } catch (JDOObjectNotFoundException e) {} 
  }
}
