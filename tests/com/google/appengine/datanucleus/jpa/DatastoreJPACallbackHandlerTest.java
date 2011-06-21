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
package com.google.appengine.datanucleus.jpa;

import junit.framework.TestCase;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJPACallbackHandlerTest extends TestCase {

  public void testGetParent() {
    Object o1 = new Object();
    assertNull(DatastoreJPACallbackHandler.getAttachingParent(o1));
    DatastoreJPACallbackHandler handler = new DatastoreJPACallbackHandler();
    handler.preAttach(o1);
    assertNull(DatastoreJPACallbackHandler.getAttachingParent(o1));
    Object o2 = new Object();
    handler.preAttach(o2);
    assertSame(o1, DatastoreJPACallbackHandler.getAttachingParent(o2));
    handler.postAttach(o2, o2);
    assertNull(DatastoreJPACallbackHandler.getAttachingParent(o1));
    handler.postAttach(o1, o1);
    assertNull(DatastoreJPACallbackHandler.getAttachingParent(o1));
  }
}
