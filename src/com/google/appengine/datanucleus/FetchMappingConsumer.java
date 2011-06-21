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
package com.google.appengine.datanucleus;

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
