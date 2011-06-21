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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreServiceConfig;

import junit.framework.TestCase;

import java.util.Arrays;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreManagerTest extends TestCase {

  public void testArraysAsListResult() {
    // DatastoreManager.addTypeManagerMappings() depends on this.
    assertEquals("java.util.Arrays$ArrayList", Arrays.asList(1, 2, 3).getClass().getName());
  }

  public void testCopyDatastoreServiceConfig() {
    DatastoreServiceConfig original = DatastoreServiceConfig.Builder.withDefaults();
    DatastoreServiceConfig copy = DatastoreManager.copyDatastoreServiceConfig(original);
    assertFalse(original == copy);
    assertEquals(original.getDeadline(), copy.getDeadline());
    assertEquals(original.getImplicitTransactionManagementPolicy(),
                 copy.getImplicitTransactionManagementPolicy());
    assertEquals(original.getReadPolicy(), copy.getReadPolicy());
  }
}
