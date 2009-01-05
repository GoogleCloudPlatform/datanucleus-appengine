// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.mapped.DatastoreIdentifier;

/**
 * Describes a 'kind' in the datastore.  This is datanucleus'
 * name for it, not the actual kind of the entities we store.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreKind implements DatastoreIdentifier {

  private final String identifier;

  public DatastoreKind(AbstractClassMetaData acmd) {
    this.identifier = acmd.getFullClassName();
  }

  public String getIdentifierName() {
    return identifier;
  }

  public String getFullyQualifiedName(boolean adapterCase) {
    // TODO(maxr): Figure out what to return here.
    return getIdentifierName();
  }

  public void setCatalogName(String s) {
  }

  public void setSchemaName(String s) {
  }

  public String getCatalogName() {
    return null;
  }

  public String getSchemaName() {
    return null;
  }
}
