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
import org.datanucleus.metadata.InheritanceStrategy;

/**
 * Series of utilities for interrogating metadata, particularly for GAE/J extensions.
 */
public class MetaDataUtils {

  public static boolean hasEncodedPKField(AbstractClassMetaData acmd) {
    int pkFieldNumber = acmd.getPKMemberPositions()[0]; // TODO Cater for composite PKs
    return isEncodedPKField(acmd, pkFieldNumber);
  }

  public static boolean isEncodedPKField(AbstractClassMetaData acmd, int fieldNumber) {
    return isEncodedPKField(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
  }

  public static boolean isEncodedPKField(AbstractMemberMetaData ammd) {
    return ammd.hasExtension(DatastoreManager.ENCODED_PK);
  }

  public static boolean isParentPKField(AbstractClassMetaData acmd, int fieldNumber) {
    return isParentPKField(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
  }

  public static boolean isParentPKField(AbstractMemberMetaData ammd) {
    return ammd.hasExtension(DatastoreManager.PARENT_PK);
  }

  public static boolean isPKNameField(AbstractClassMetaData acmd, int fieldNumber) {
    return isPKNameField(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
  }

  public static boolean isPKNameField(AbstractMemberMetaData ammd) {
    return ammd.hasExtension(DatastoreManager.PK_NAME);
  }

  public static boolean isPKIdField(AbstractClassMetaData acmd, int fieldNumber) {
    return isPKIdField(acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
  }

  public static boolean isPKIdField(AbstractMemberMetaData ammd) {
    return ammd.hasExtension(DatastoreManager.PK_ID);
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
}