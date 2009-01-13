// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.query.UnionIteratorStatement;

/**
 * Datastore-specific extension to {@link UnionIteratorStatement}.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreUnionIteratorStatement extends UnionIteratorStatement {

  DatastoreUnionIteratorStatement(ClassLoaderResolver clr, Class candidateType, boolean includeSubclasses,
                                  StoreManager storeMgr, Class sourceType, JavaTypeMapping sourceMapping,
                                  DatastoreContainerObject sourceTable, boolean sourceJoin, Boolean withMetadata,
                                  boolean joinToExcludeTargetSubclasses, boolean allowsNull) {
    super(clr, candidateType, includeSubclasses, storeMgr, sourceType, sourceMapping, sourceTable,
          sourceJoin, withMetadata, joinToExcludeTargetSubclasses, allowsNull);
  }

  protected boolean sourceTableIsJoinTable() {
    // Currently no support for join tables.
    return false;
  }
}
