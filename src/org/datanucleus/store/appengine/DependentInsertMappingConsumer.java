// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.MappingCallbacks;
import org.datanucleus.store.mapped.mapping.MappingConsumer;
import org.datanucleus.store.mapped.mapping.PersistenceCapableMapping;
import org.datanucleus.store.mapped.mapping.ReferenceMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link MappingConsumer} implementation that is used for the insert of dependent
 * objects.  Largely based on InsertMappingConsumer.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DependentInsertMappingConsumer implements MappingConsumer {

  /**
   * Numbers of all relations fields (bidir that may already be attached when persisting).
   */
  private final List<Integer> relationFields = new ArrayList<Integer>();

  private final List<MappingCallbacks> mc = new ArrayList<MappingCallbacks>();

  private boolean initialized = false;

  private final ClassLoaderResolver clr;
  private final AbstractClassMetaData cmd;

  public DependentInsertMappingConsumer(ClassLoaderResolver clr, AbstractClassMetaData cmd) {
    this.clr = clr;
    this.cmd = cmd;
  }

  public void preConsumeMapping(int highestFieldNumber) {
    if (!initialized) {
      initialized = true;
    }
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
      int relationType = fmd.getRelationType(clr);
      if (relationType == Relation.ONE_TO_ONE_BI) {
      } else if (relationType == Relation.MANY_TO_ONE_BI) {
        AbstractMemberMetaData[] relatedMmds = fmd.getRelatedMemberMetaData(clr);
        if (fmd.getJoinMetaData() == null && relatedMmds[0].getJoinMetaData() == null) {
          // N-1 bidirectional field using FK (in this table)
          relationFields.add(fmd.getAbsoluteFieldNumber());
        }
      }
    }
    if (m instanceof MappingCallbacks) {
      mc.add((MappingCallbacks) m);
    }
  }

  public void consumeMapping(JavaTypeMapping m, int mappingType) {
  }

  public void consumeUnmappedDatastoreField(DatastoreField fld) {
  }

  public List<MappingCallbacks> getMappingCallbacks() {
    return mc;
  }

  /**
   * Accessor for the numbers of the relation fields (not inserted).
   *
   * @return the array of field numbers
   */
  public int[] getRelationFieldNumbers() {
    int[] fieldNumbers = new int[relationFields.size()];
    for (int i = 0; i < relationFields.size(); ++i) {
      fieldNumbers[i] = relationFields.get(i);
    }
    return fieldNumbers;
  }
}