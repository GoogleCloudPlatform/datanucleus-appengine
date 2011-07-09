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
package com.google.appengine.datanucleus.jdo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.appengine.datanucleus.test.Flight;

import javax.jdo.JDOHelper;
import javax.jdo.Transaction;

import org.datanucleus.util.NucleusLogger;

public class JDODummyTest extends JDOTestCase {
  public void testTransactionClosing_NonTx() {
    // Create some data
    List ids = new ArrayList();
    for (int i=0;i<100;i++) {
        Transaction tx = pm.currentTransaction();
        tx.begin();
      NucleusLogger.GENERAL.info(">> Persisting Flight " + i);
      Flight flight = new Flight("Start"+i, "End"+i, "Flight " + i, 5, 4);
      pm.makePersistent(flight);
      ids.add(JDOHelper.getObjectId(flight));
      tx.commit();
    }
    pm.close();
    pmf.getDataStoreCache().evictAll();

    Iterator iter = ids.iterator();
    while (iter.hasNext())
    {
      Object id = iter.next();
      NucleusLogger.GENERAL.info(">> Retrieving Flight with id=" + id);
      pm = pmf.getPersistenceManager();
      pm.currentTransaction().setNontransactionalRead(true);
      pm.getObjectById(id);
      pm.close();
    }
  }
}
