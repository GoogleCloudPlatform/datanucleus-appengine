// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ObjectManager;
import org.datanucleus.store.query.ResultObjectFactory;

/**
 * Datastore-specific implementation of {@link ResultObjectFactory}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreResultObjectFactory implements ResultObjectFactory {

  public Object getObject(ObjectManager om, Object obj) {
    // TODO(maxr)
    throw new UnsupportedOperationException();
  }
}
