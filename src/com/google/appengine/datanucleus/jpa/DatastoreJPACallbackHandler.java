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

import org.datanucleus.jpa.JPACallbackHandler;

import java.util.LinkedList;

/**
 * Datastore-specific extension of {@link JPACallbackHandler}
 * that maintains a stack of objects that are being attached.
 * This is necessary to support the situation where someone
 * is adding unidirectional child objects to a detached instance
 * and then merging (see
 * JPAAttachDetachTest.testSerializeWithOneToMany_AddGrandchildToUnidirectionalDetached)
 * for an example).  In this situation we don't have access to the
 * owning object when we receive the call to insert the child, and as a
 * result we have no way of putting it in the correct entity group.
 * In order to get access to the owning object we maintain a stack of
 * objects that are being attached, and we walk the stack until
 * we find the first object that is not ourselves and treat that
 * as our parent.
 *
 * Like many other solutions, this one is pretty fragile but
 * it does the job for DataNuc 1.1.4.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJPACallbackHandler extends JPACallbackHandler {

  /**
   * Our stack of objects that are being attached.  Can't use ArrayDeque
   * because we need to stay java 5 compatible.  Not using Stack because
   * we need the ability to iterate.
   */
  private static final ThreadLocal<LinkedList<Object>> attaching =
      new ThreadLocal<LinkedList<Object>>() {
    @Override
    protected LinkedList<Object> initialValue() {
      return new LinkedList<Object>();
    }
  };

  @Override
  public void preAttach(Object pc) {
    super.preAttach(pc);
    // push the object on the stack.
    attaching.get().addFirst(pc);
  }

  @Override
  public void postAttach(Object pc, Object detachedPC) {
    LinkedList<Object> list = attaching.get();
    while (!list.isEmpty()) {
      // Keep popping until we pop the same obj that was passed in.
      // This is to account for the possibility that there's extra
      // junk in the list that didn't get cleaned up properly.
      if (detachedPC == list.removeFirst()) {
        break;
      }
    }
    super.postAttach(pc, detachedPC);
  }

  /**
   * Returns the first object in the list that is not the
   * child passed in.
   */
  public static Object getAttachingParent(Object child) {
    for (Object obj : attaching.get()) {
      if (obj != child) {
        return obj;
      }
    }
    // not found, this is fine
    return null;
  }

  static void clearAttaching() {
    attaching.get().clear();
  }
}
