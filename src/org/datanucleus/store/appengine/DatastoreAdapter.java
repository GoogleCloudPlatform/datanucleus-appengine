// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.rdbms.adapter.DatabaseAdapter;

import java.sql.DatabaseMetaData;

/**
 * Adapter for the App Engine datastore.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreAdapter extends DatabaseAdapter {

  public DatastoreAdapter(DatabaseMetaData metadata) {
    super(metadata);
  }

}
