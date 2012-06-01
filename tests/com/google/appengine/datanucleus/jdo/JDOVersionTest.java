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

import java.sql.Timestamp;

import javax.jdo.JDOHelper;

import junit.framework.Assert;

import com.google.appengine.datanucleus.test.jdo.TimestampVersionJDO;

/**
 * Tests for JDO versioning.
 */
public class JDOVersionTest extends JDOTestCase {

  public void testTimestampVersion() {
    pm.currentTransaction().begin();
    TimestampVersionJDO tv = new TimestampVersionJDO();
    tv.setStr1("First Value");
    pm.makePersistent(tv);
    pm.currentTransaction().commit();
    Object firstVersion = JDOHelper.getVersion(tv);
    Assert.assertTrue(firstVersion instanceof Timestamp);

    pm.currentTransaction().begin();
    tv.setStr1("Second Value");
    pm.currentTransaction().commit();
    Object secondVersion = JDOHelper.getVersion(tv);
    Assert.assertTrue(secondVersion instanceof Timestamp);
    long firstMillis = ((Timestamp)firstVersion).getTime();
    long secondMillis = ((Timestamp)secondVersion).getTime();
    Assert.assertTrue(secondMillis > firstMillis);
  }
}
