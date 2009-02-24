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
