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
package org.datanucleus.store.appengine;

import com.google.apphosting.api.ApiProxy;

/**
 * An extension to {@link ApiProxy.Delegate} that knows how to set
 * itself up and tear itself down.  Used by {@link DatastoreTestHelper} to
 * allow the orm test-suite to be run using the local datastore that ships with
 * the sdk and any other datastore implementation that can be wrapped in this
 * interface (like the real datastore for example).
 *
 * @author Max Ross <maxr@google.com>
 */
interface DatastoreDelegate extends ApiProxy.Delegate {

  void setUp() throws Exception;

  void tearDown() throws Exception;
}
