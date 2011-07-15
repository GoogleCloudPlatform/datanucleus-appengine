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
package com.google.appengine.datanucleus.jpa;

import java.util.Calendar;
import java.util.Date;

import javax.persistence.Query;

import junit.framework.Assert;

import com.google.appengine.datanucleus.test.TemporalHolder;

/**
 * Tests for storage of temporal types
 */
public class JPATemporalTest extends JPATestCase {

  @SuppressWarnings("deprecation")
public void testInsert_IdGen() {
    int year = 2010;
    int month = 5;
    int day_of_month = 15;
    int hour_of_day = 5;
    int minute = 10;
    int second = 45;

    TemporalHolder holder = new TemporalHolder(1);
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, year);
    cal.set(Calendar.MONTH, month);
    cal.set(Calendar.DAY_OF_MONTH, day_of_month);
    cal.set(Calendar.HOUR_OF_DAY, hour_of_day);
    cal.set(Calendar.MINUTE, minute);
    cal.set(Calendar.SECOND, second);
    cal.set(Calendar.MILLISECOND, 0);
    holder.setDateField(cal.getTime());
    holder.setTimestampField(cal.getTime());
    holder.setTimeField(cal.getTime());

    beginTxn();
    em.persist(holder);
    commitTxn();
    em.clear(); // Clean L1 cache to enforce load from datastore

    // Retrieve and check fields
    beginTxn();
    Query q = em.createQuery("SELECT h FROM TemporalHolder h WHERE id = 1");
    TemporalHolder h = (TemporalHolder)q.getSingleResult();
    Date dateField = h.getDateField();
    Assert.assertEquals("Year of Date incorrect", year, dateField.getYear()+1900);
    Assert.assertEquals("Month of Date incorrect", month, dateField.getMonth());
    Assert.assertEquals("Day of Date incorrect", day_of_month, dateField.getDate());
    Date timeField = h.getTimeField();
    Assert.assertEquals("Hour of Time incorrect", hour_of_day, timeField.getHours());
    Assert.assertEquals("Minute of Time incorrect", minute, timeField.getMinutes());
    Assert.assertEquals("Second of Time incorrect", second, timeField.getSeconds());
    Date timestampField = h.getTimestampField();
    Assert.assertEquals("Year of Timestamp incorrect", year, timestampField.getYear()+1900);
    Assert.assertEquals("Month of Timestamp incorrect", month, timestampField.getMonth());
    Assert.assertEquals("Day of Timestamp incorrect", day_of_month, timestampField.getDate());
    Assert.assertEquals("Hour of Timestamp incorrect", hour_of_day, timestampField.getHours());
    Assert.assertEquals("Minute of Timestamp incorrect", minute, timestampField.getMinutes());
    Assert.assertEquals("Second of Timestamp incorrect", second, timestampField.getSeconds());
    commitTxn();
  }
}