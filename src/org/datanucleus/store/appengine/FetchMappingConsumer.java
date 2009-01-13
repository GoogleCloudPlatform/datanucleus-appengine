// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.MappingConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
class FetchMappingConsumer implements MappingConsumer {

  private final List<MappingCallbacks> mappingCallbacks = new ArrayList<MappingCallbacks>();

  private final AbstractClassMetaData cmd;

  public List<MappingCallbacks> getMappingCallbacks() {
    return mappingCallbacks;
  }

  FetchMappingConsumer(AbstractClassMetaData cmd) {
    this.cmd = cmd;
  }

  public void consumeMapping(JavaTypeMapping m, AbstractMemberMetaData fmd) {
    if (!fmd.getAbstractClassMetaData().isSameOrAncestorOf(cmd)) {
      return;
    }
    if (m instanceof MappingCallbacks) {
      mappingCallbacks.add((MappingCallbacks) m);
    }
  }

  public void preConsumeMapping(int highestFieldNumber) {
  }

  public void consumeMapping(JavaTypeMapping m, int mappingType) {
  }

  public void consumeUnmappedDatastoreField(DatastoreField fld) {
  }
}
