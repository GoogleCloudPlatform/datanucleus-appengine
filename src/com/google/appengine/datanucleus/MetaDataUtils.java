/**********************************************************************
Copyright (c) 2011 Google Inc.

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
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.InheritanceStrategy;

/**
 * Series of utilities for interrogating metadata, particularly for GAE/J extensions.
 */
public class MetaDataUtils {

  private static boolean memberHasExtension(AbstractClassMetaData acmd, int fieldNumber, String extensionName) {
    return acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).hasExtension(extensionName);
  }

  public static boolean hasEncodedPKField(AbstractClassMetaData acmd) {
    int pkFieldNumber = acmd.getPKMemberPositions()[0]; // TODO Cater for composite PKs
    return isEncodedPKField(acmd, pkFieldNumber);
  }

  public static boolean isEncodedPKField(AbstractClassMetaData acmd, int fieldNumber) {
    return memberHasExtension(acmd, fieldNumber, DatastoreManager.ENCODED_PK);
  }

  public static boolean isParentPKField(AbstractClassMetaData acmd, int fieldNumber) {
    return memberHasExtension(acmd, fieldNumber, DatastoreManager.PARENT_PK);
  }

  public static boolean isPKNameField(AbstractClassMetaData acmd, int fieldNumber) {
    return memberHasExtension(acmd, fieldNumber, DatastoreManager.PK_NAME);
  }

  public static boolean isPKIdField(AbstractClassMetaData acmd, int fieldNumber) {
    return memberHasExtension(acmd, fieldNumber, DatastoreManager.PK_ID);
  }

  public static boolean isNewOrSuperclassTableInheritanceStrategy(AbstractClassMetaData cmd) {
    while (cmd != null) {
      AbstractClassMetaData pcmd = cmd.getSuperAbstractClassMetaData();
      if (pcmd == null) {
        return cmd.getInheritanceMetaData().getStrategy() == InheritanceStrategy.NEW_TABLE;
      }
      else if (cmd.getInheritanceMetaData().getStrategy() != InheritanceStrategy.SUPERCLASS_TABLE) {
        return false;
      }
      cmd = pcmd;
    }
    return false;
  }

  /**
   * Returns whether the specified member (or element of collection/array member) is insertable.
   * @param ammd Metadata for the member
   * @return Whether it is considered insertable
   */
  public static boolean isMemberInsertable(AbstractMemberMetaData ammd) {
    ColumnMetaData[] colmds = null;
    if (ammd.hasCollection() || ammd.hasArray()) {
      if (ammd.getElementMetaData() != null && ammd.getElementMetaData().getColumnMetaData() != null &&
          ammd.getElementMetaData().getColumnMetaData().length > 0) {
        colmds = ammd.getElementMetaData().getColumnMetaData();
      }
    }
    else if (ammd.hasMap()) {
      // TODO Support maps
    }
    else {
      colmds = ammd.getColumnMetaData();
    }

    if (colmds != null && colmds.length > 0) {
      return colmds[0].getInsertable();
    }
    return true;
  }

  /**
   * Returns whether the specified member (or element of collection/array member) is updateable.
   * @param ammd Metadata for the member
   * @return Whether it is considered updateable
   */
  public static boolean isMemberUpdatable(AbstractMemberMetaData ammd) {
    ColumnMetaData[] colmds = null;
    if (ammd.hasCollection() || ammd.hasArray()) {
      if (ammd.getElementMetaData() != null && ammd.getElementMetaData().getColumnMetaData() != null &&
          ammd.getElementMetaData().getColumnMetaData().length > 0) {
        colmds = ammd.getElementMetaData().getColumnMetaData();
      }
    }
    else if (ammd.hasMap()) {
      // TODO Support maps
    }
    else {
      colmds = ammd.getColumnMetaData();
    }

    if (colmds != null && colmds.length > 0) {
      return colmds[0].getUpdateable();
    }
    return true;
  }
}