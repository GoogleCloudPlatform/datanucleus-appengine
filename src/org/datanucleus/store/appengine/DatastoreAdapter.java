// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.store.mapped.mapping.MappingManager;
import org.datanucleus.store.rdbms.adapter.DatabaseAdapter;

import java.sql.DatabaseMetaData;

/**
 * Adapter for the App Engine datastore.
 * TODO(maxr): Don't extend rdbms.adapter.DatabaseAdapter
 * Currently necessary to get the identifier factory stuff
 * to work.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAdapter extends DatabaseAdapter {

  public DatastoreAdapter(DatabaseMetaData metadata) {
    super(metadata);
    supportedOptions.add(IDENTITY_COLUMNS);
  }

  @Override
  protected MappingManager getNewMappingManager() {
    return new DatastoreMappingManager();
  }
}
