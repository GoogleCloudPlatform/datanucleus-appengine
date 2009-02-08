// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.store.mapped.mapping.ReferenceMapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
  private AbstractMemberMetaData ancestorMapping = null;

  private final AbstractClassMetaData cmd;

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
    if (m.getNumberOfDatastoreFields() == 0 &&
        (m instanceof PersistenceCapableMapping || m instanceof ReferenceMapping)) {
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

  List<MappingCallbacks> getMappingCallbacks() {
    return mc;
  }

  Set<JavaTypeMapping> getExternalFKMappings() {
    return externalFKMappings;
  }

  Set<JavaTypeMapping> getExternalOrderMappings() {
    return externalOrderMappings;
  }

  AbstractMemberMetaData getAncestorMappingField() {
    return ancestorMapping;
  }

  void setAncestorMappingField(AbstractMemberMetaData ancestorMapping) {
    this.ancestorMapping = ancestorMapping;
  }
}