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

import java.util.Currency;
import java.util.Locale;
import java.util.TimeZone;

import com.google.appengine.datanucleus.test.jdo.HasExtraTypes1JDO;

/**
 * Tests for persistence of various non-standard Java types
 */
public class JDOTypesTest extends JDOTestCase {

  public void testTypes1() {
    HasExtraTypes1JDO holder = new HasExtraTypes1JDO();
    holder.setCurrency(Currency.getInstance("GBP"));
    holder.setLocale(Locale.ENGLISH);
    holder.setStringBuffer(new StringBuffer("sausages"));
    holder.setTimezone(TimeZone.getTimeZone("GMT"));

    beginTxn();
    pm.makePersistent(holder);
    commitTxn();
    Object id = pm.getObjectId(holder);
    pm.evictAll();

    beginTxn();
    HasExtraTypes1JDO extra = (HasExtraTypes1JDO)pm.getObjectById(id);
    assertEquals("GBP", extra.getCurrency().getCurrencyCode());
    assertEquals("en", extra.getLocale().toString());
    assertEquals("sausages", extra.getStringBuffer().toString());
    assertEquals("GMT", extra.getTimezone().getID());
    commitTxn();
  }
}
