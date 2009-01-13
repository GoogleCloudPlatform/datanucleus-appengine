// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.StateManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.exceptions.MappedDatastoreException;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.query.DiscriminatorIteratorStatement;
import org.datanucleus.store.mapped.query.UnionIteratorStatement;
import org.datanucleus.store.mapped.scostore.AbstractCollectionStore;
import org.datanucleus.store.mapped.scostore.AbstractCollectionStoreSpecialization;
import org.datanucleus.store.mapped.scostore.ElementContainerStore;
import org.datanucleus.util.Localiser;

/**
 * Datastore-specific implementation of
 * {@link AbstractCollectionStoreSpecialization}.
 * 
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAbstractCollectionStoreSpecialization 
    extends DatastoreElementContainerStoreSpecialization
    implements AbstractCollectionStoreSpecialization {

  DatastoreAbstractCollectionStoreSpecialization(Localiser localiser, ClassLoaderResolver clr,
                                                 DatastoreManager storeMgr) {
    super(localiser, clr, storeMgr);
  }

  public boolean updateEmbeddedElement(StateManager sm, Object element, int fieldNumber,
                                       Object value, JavaTypeMapping fieldMapping,
                                       MappedStoreManager storeMgr, ElementContainerStore ecs) {
    // TODO(maxr)
    return false;
  }

  public boolean contains(StateManager sm, Object element, AbstractCollectionStore acs) {
    // TODO(maxr)
    return false;
  }

  public int[] internalRemove(StateManager ownerSM, ManagedConnection conn, boolean batched,
                              Object element, boolean executeNow, AbstractCollectionStore acs)
      throws MappedDatastoreException {
    // TODO(maxr)
    return new int[0];
  }

  public DiscriminatorIteratorStatement newDiscriminatorIteratorStatement(ClassLoaderResolver clr,
                                                                          Class[] cls,
                                                                          boolean includeSubclasses,
                                                                          MappedStoreManager storeMgr,
                                                                          boolean selectDiscriminator) {
    // TODO(maxr)
    return null;
  }

  public DiscriminatorIteratorStatement newDiscriminatorIteratorStatement(ClassLoaderResolver clr,
                                                                          Class[] cls, boolean b,
                                                                          MappedStoreManager storeMgr,
                                                                          boolean b1,
                                                                          boolean allowsNull,
                                                                          DatastoreContainerObject containerTable,
                                                                          JavaTypeMapping elementMapping,
                                                                          DatastoreIdentifier elmIdentifier) {
    // TODO(maxr)
    return null;
  }

  public UnionIteratorStatement newUnionIteratorStatement(ClassLoaderResolver clr,
                                                          Class candidateType,
                                                          boolean includeSubclasses,
                                                          StoreManager storeMgr, Class sourceType,
                                                          JavaTypeMapping sourceMapping,
                                                          DatastoreContainerObject sourceTable,
                                                          boolean sourceJoin, Boolean withMetadata,
                                                          boolean joinToExcludeTargetSubclasses,
                                                          boolean allowsNull) {
    return new DatastoreUnionIteratorStatement(clr, candidateType, includeSubclasses, storeMgr,
                                               sourceType, sourceMapping, sourceTable, sourceJoin,
                                               withMetadata, joinToExcludeTargetSubclasses,
                                               allowsNull);
  }
}
