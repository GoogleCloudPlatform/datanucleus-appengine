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

import java.lang.reflect.Method;

/**
 * Policy for disabling datastore writes.  Useful for proving
 * that operations that shouldn't write to the datastore aren't
 * actually writing to the datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public final class WriteBlocker implements DatastoreServiceInterceptor.Policy {

  public void intercept(Object o, Method method, Object[] params) {
    if (method.getName().equals("put") || method.getName().equals("delete")) {
      throw new RuntimeException("Detected a write: " + method);
    }
  }
}

