// Copyright 2009 Google Inc. All Rights Reserved.
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
