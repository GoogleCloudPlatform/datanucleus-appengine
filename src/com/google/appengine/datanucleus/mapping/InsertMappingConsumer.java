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
package com.google.appengine.datanucleus.mapping;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.mapped.mapping.ReferenceMapping;

import com.google.appengine.datanucleus.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link MappingConsumer} implementation that is used for the insertions.
 * Largely based on InsertMappingConsumer.
 *
 * @author Max Ross <maxr@google.com>
 */
public class InsertMappingConsumer implements MappingConsumer {

  private final Set<JavaTypeMapping> externalFKMappings = new HashSet<JavaTypeMapping>();
  private final Set<JavaTypeMapping> externalOrderMappings = new HashSet<JavaTypeMapping>();
  private final List<MappingCallbacks> mc = new ArrayList<MappingCallbacks>();
  private final Map<Integer, AbstractMemberMetaData> fieldIndexToMemberMetaData = Utils.newHashMap();
  private final AbstractClassMetaData cmd;

  private AbstractMemberMetaData parentMapping = null;
  // running tally of the number of fields we've consumed
  // we assume that fields are consumed in fieldIndex order
  private int numFields = 0;

  public InsertMappingConsumer(AbstractClassMetaData cmd) {
    this.cmd = cmd;
  }

  public void preConsumeMapping(int highestFieldNumber) {
  }

  public void consumeMapping(JavaTypeMapping m, AbstractMemberMetaData fmd) {
    if (!fmd.getAbstractClassMetaData().isSameOrAncestorOf(cmd)) {
      // Make sure we only accept mappings from the correct part of any inheritance tree
      return;
    }
    if (m.getNumberOfDatastoreMappings() == 0 &&
        (m instanceof PersistableMapping || m instanceof ReferenceMapping)) {
      // Reachable Fields (that relate to this object but have no column in the table)
    } else {
      // Fields to be "inserted" (that have a datastore column)
      // Check if the field is "insertable" (either using JPA column, or JDO extension)
      if (fmd.hasExtension("insertable") && fmd.getValueForExtension("insertable").equalsIgnoreCase("false")) {
        return;
      }
      ColumnMetaData[] colmds = fmd.getColumnMetaData();
      if (colmds != null && colmds.length > 0) {
        for (ColumnMetaData colmd : colmds) {
          if (!colmd.getInsertable()) {
            // Not to be inserted
            return;
          }
        }
      }
    }

    fieldIndexToMemberMetaData.put(numFields++, fmd);

    if (m instanceof MappingCallbacks) {
        mc.add((MappingCallbacks) m);
    }
  }

  public void consumeMapping(JavaTypeMapping m, int mappingType) {
    if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_FK) {
      // External FK mapping (1-N uni)
      externalFKMappings.add(m);
    } else if (mappingType == MappingConsumer.MAPPING_TYPE_EXTERNAL_INDEX) {
      // External FK order mapping (1-N uni List)
      externalOrderMappings.add(m);
    }
  }

  public void consumeUnmappedDatastoreField(DatastoreField fld) {
  }

  public List<MappingCallbacks> getMappingCallbacks() {
    return mc;
  }

  public Set<JavaTypeMapping> getExternalFKMappings() {
    return externalFKMappings;
  }

  public Set<JavaTypeMapping> getExternalOrderMappings() {
    return externalOrderMappings;
  }

  public AbstractMemberMetaData getParentMappingField() {
    return parentMapping;
  }

  void setParentMappingField(AbstractMemberMetaData parentMapping) {
    this.parentMapping = parentMapping;
  }

  public AbstractMemberMetaData getMemberMetaDataForIndex(int index) {
    return fieldIndexToMemberMetaData.get(index);
  }

  public Map<Integer, AbstractMemberMetaData> getFieldIndexToMemberMetaData() {
    return fieldIndexToMemberMetaData;
  }
}