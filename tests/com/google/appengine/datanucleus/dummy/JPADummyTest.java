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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.google.appengine.datanucleus.jpa.JPATestCase;
import com.google.appengine.datanucleus.test.TemporalHolder;

import org.datanucleus.util.NucleusLogger;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPADummyTest extends JPATestCase {

  public void testQueryGreaterThanDate() {
    beginTxn();
    TemporalHolder h1 = new TemporalHolder(1);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, 2009);
    cal.set(Calendar.MONTH, 3);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    h1.setDateField(cal.getTime());
    h1.setTimeField(cal.getTime());
    h1.setTimestampField(cal.getTime());
    em.persist(h1);
    commitTxn();
    beginTxn();
    TemporalHolder h2 = new TemporalHolder(2);
    cal.set(Calendar.YEAR, 2012);
    cal.set(Calendar.MONTH, 5);
    cal.set(Calendar.DAY_OF_MONTH, 1);
    h2.setDateField(cal.getTime());
    h2.setTimeField(cal.getTime());
    h2.setTimestampField(cal.getTime());
    em.persist(h2);
    commitTxn();

    // Should only match one
    beginTxn();
    Date date = new Date();
    javax.persistence.Query q = em.createQuery("SELECT h FROM TemporalHolder h WHERE h.timestampField > :value");
    q.setParameter("value", date);
    List<TemporalHolder> holders = q.getResultList();
    NucleusLogger.GENERAL.info(">> num.results=" + holders.size());
    commitTxn();
  }
}
