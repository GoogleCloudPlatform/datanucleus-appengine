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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InheritanceStrategy;
import org.datanucleus.metadata.MetaDataManager;

/**
 * Series of utilities for interrogating metadata, particularly for GAE/J extensions.
 */
public class MetaDataUtils {

  /**
   * Accessor for the default value specified for the provided member.
   * If no defaultValue is provided on the column then returns null.
   * @param mmd Metadata for the member
   * @return The default value
   */
  public static String getDefaultValueForMember(AbstractMemberMetaData mmd)
  {
      ColumnMetaData[] colmds = mmd.getColumnMetaData();
      if (colmds == null || colmds.length < 1)
      {
          return null;
      }
      return colmds[0].getDefaultValue();
  }

  public static boolean hasEncodedPKField(AbstractClassMetaData acmd) {
    if (acmd.getIdentityType() == IdentityType.DATASTORE) {
      IdentityMetaData idmd = acmd.getIdentityMetaData();
      return idmd.hasExtension(DatastoreManager.ENCODED_PK);
    } else {
      int pkFieldNumber = acmd.getPKMemberPositions()[0]; // TODO Cater for composite PKs
      return isEncodedPKField(acmd, pkFieldNumber);
    }
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

  /**
   * Convenience accessor for whether this relation is owned (or not).
   * Currently assumes owned unless otherwise specified.
   * @param mmd Metadata for the field/property
   * @param storeMgr StoreManager
   * @return Whether it is owned
   */
  public static boolean isOwnedRelation(AbstractMemberMetaData mmd, DatastoreManager storeMgr) {
    if (storeMgr.isDefaultToOwnedRelations()) {
      // Defaulting to owned unless specified otherwise
      if ("true".equalsIgnoreCase(mmd.getValueForExtension("gae.unowned"))) {
        return false;
      }
      return true;
    } else {
      // Defaulting to unowned unless specified otherwise
      if ("false".equalsIgnoreCase(mmd.getValueForExtension("gae.unowned"))) {
        return true;
      }
      return false;
    }
  }

  /**
   * Convenience method to return whether or not keys of related objects can be
   * expected to exist on the parent
   */
  public static boolean readRelatedKeysFromParent(DatastoreManager storeMgr, AbstractMemberMetaData mmd) {
    return !MetaDataUtils.isOwnedRelation(mmd, storeMgr) ||
        storeMgr.storageVersionAtLeast(StorageVersion.READ_OWNED_CHILD_KEYS_FROM_PARENTS);
  }

  /**
   * Convenience method to return the metadata for the field/property of this class that stores the 
   * parent PK (in metadata as "gae.parent-pk").
   * @param cmd Metadata for the class
   * @return The parent PK member metadata (or null, if none)
   */
  public static AbstractMemberMetaData getParentPkMemberMetaDataForClass(AbstractClassMetaData cmd, 
      MetaDataManager mmgr, ClassLoaderResolver clr) {
    AbstractMemberMetaData[] mmds = cmd.getManagedMembers();
    for (int i=0;i<mmds.length;i++) {
      if (MetaDataUtils.isParentPKField(mmds[i])) {
        return mmds[i];
      }
      else if (mmds[i].getEmbeddedMetaData() != null) {
        // TODO Doubtful if this is really correct. The parent key of this class should not be in an embedded class
        // since the parent of the embedded class is this class. What if we had two instances of this class embedded?
        AbstractClassMetaData embCmd = mmgr.getMetaDataForClass(mmds[i].getType(), clr);
        AbstractMemberMetaData embPkParentMmd = getParentPkMemberMetaDataForClass(embCmd, mmgr, clr);
        if (embPkParentMmd != null) {
          return embPkParentMmd;
        }
      }
    }

    AbstractClassMetaData superCmd = cmd.getSuperAbstractClassMetaData();
    if (superCmd != null) {
      return getParentPkMemberMetaDataForClass(superCmd, mmgr, clr);
    }
    return null;
  }
}