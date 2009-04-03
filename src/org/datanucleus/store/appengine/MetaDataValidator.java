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

import com.google.appengine.api.datastore.Key;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.Relation;
import org.datanucleus.util.NucleusLogger;

import java.util.Set;

/**
 * App Engine specific rules for Meta Data.
 *
 * @author Max Ross <maxr@google.com>
 */
public class MetaDataValidator {

  private static final Set<String> ONE_OR_ZERO_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.ENCODED_PK,
          DatastoreManager.PK_NAME,
          DatastoreManager.PARENT_PK);

  private static final Set<String> NOT_PRIMARY_KEY_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME,
          DatastoreManager.PARENT_PK);

  private static final Set<String> REQUIRES_ENCODED_STRING_PK_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME);

  private final AbstractClassMetaData acmd;
  private final MetaDataManager metaDataManager;
  private final ClassLoaderResolver clr;

  private boolean noParentAllowed = false;
  public MetaDataValidator(
      AbstractClassMetaData acmd, MetaDataManager metaDataManager, ClassLoaderResolver clr) {
    this.acmd = acmd;
    this.metaDataManager = metaDataManager;
    this.clr = clr;
  }

  public void validate() {
    NucleusLogger.METADATA.info(
        "Performing appengine-specific metadata validation for " + acmd.getFullClassName());
    AbstractMemberMetaData pkMemberMetaData = validatePrimaryKey();
    validateExtensions(acmd, pkMemberMetaData);
    NucleusLogger.METADATA.info(
        "Finished performing appengine-specific metadata validation for " + acmd.getFullClassName());
  }

  private void validateExtensions(
      AbstractClassMetaData acmd, AbstractMemberMetaData pkMemberMetaData) {
    Set<String> foundOneOrZeroExtensions = Utils.newHashSet();
    Class<?> pkClass = pkMemberMetaData.getType();

    for (AbstractMemberMetaData ammd : acmd.getManagedMembers()) {
      // can only have one field with this extension
      for (String extension : ONE_OR_ZERO_EXTENSIONS) {
        if (ammd.hasExtension(extension)) {
          if (!foundOneOrZeroExtensions.add(extension)) {
            throw new DatastoreMetaDataException(acmd, ammd,
                "Cannot have more than one field with the \"" + extension
                + "\" extension.");
          }
        }
      }

      // encoded-pk must be on a String pk field
      if (ammd.hasExtension(DatastoreManager.ENCODED_PK)) {
        if (!ammd.isPrimaryKey() || !ammd.getType().equals(String.class)) {
          throw new DatastoreMetaDataException(
              acmd, ammd,
              "A field with the \"" + DatastoreManager.ENCODED_PK + "\" extension can only be "
              + "applied to a String primary key.");
        }
      }

      if (ammd.hasExtension(DatastoreManager.PK_NAME)) {
        if (!ammd.getType().equals(String.class)) {
          throw new DatastoreMetaDataException(
              acmd, ammd,
              "\"" + DatastoreManager.PK_NAME + "\" can only be applied to a String field.");
        }
      }
      if (ammd.hasExtension(DatastoreManager.PK_ID)) {
        if (!ammd.getType().equals(Long.class)) {
          throw new DatastoreMetaDataException(
              acmd, ammd,
              "\"" + DatastoreManager.PK_ID + "\" can only be applied to a Long field.");
        }
      }

      if (ammd.hasExtension(DatastoreManager.PARENT_PK)) {
        if (noParentAllowed) {
          throw new DatastoreMetaDataException(
              acmd, ammd, "Cannot have a " + pkClass.getName() + " primary key and a parent pk field.");
        }
        if (!ammd.getType().equals(String.class) && !ammd.getType().equals(Key.class)) {
          throw new DatastoreMetaDataException(
              acmd, ammd, "Parent pk must be of type String or " + Key.class.getName() + ".");
        }
        // JPA doesn't actually support Key members that aren't primary keys,
        // but we can't check for that here because it just gets dropped from
        // the metadata altogether.
      }

      for (String extension : NOT_PRIMARY_KEY_EXTENSIONS) {
        if (ammd.hasExtension(extension) && ammd.isPrimaryKey()) {
          throw new DatastoreMetaDataException(
              acmd, ammd,
              "A field with the \"" + extension + "\" extension must not be the primary key.");
        }
      }

      // pk-name and pk-id only supported in conjunction with an encoded string
      for (String extension : REQUIRES_ENCODED_STRING_PK_EXTENSIONS) {
        if (ammd.hasExtension(extension)) {
          if (!pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)) {
            // we've already verified that encoded-pk is on a a String pk field
            // so we don't need to check the type of the pk here.
            throw new DatastoreMetaDataException(
                acmd, ammd,
                "A field with the \"" + extension + "\" extension can only be used in conjunction "
                + "with an encoded String primary key..");
          }
        }
      }

      if (noParentAllowed) {
        checkForIllegalChildField(ammd);
      }
    }
  }

  private void checkForIllegalChildField(AbstractMemberMetaData ammd) {
    // Figure out if this field is the owning side of a one to one or a one to
    // many.  If it is, look at the mapping of the child class and make sure their
    // pk isn't Long or unencoded String.
    int relationType = ammd.getRelationType(clr);
    if (relationType == Relation.NONE || ammd.isEmbedded()) {
      return;
    }
    AbstractClassMetaData childAcmd = null;
    if (relationType == Relation.ONE_TO_MANY_BI || relationType == Relation.ONE_TO_MANY_UNI) {
      if (ammd.getCollection() != null) {
        childAcmd = ammd.getCollection().getElementClassMetaData(clr);
      } else if (ammd.getArray() != null) {
        childAcmd = ammd.getArray().getElementClassMetaData(clr);
      } else {
        // don't know how to verify
        NucleusLogger.METADATA.warn("Unable to validate one-to-many relation " + ammd.getFullFieldName());
      }
    } else if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) {
      childAcmd = metaDataManager.getMetaDataForClass(ammd.getType(), clr);
    }
    if (childAcmd == null) {
      return;
    }

    // Get the type of the primary key of the child
    int[] pkPositions = childAcmd.getPKMemberPositions();
    if (pkPositions == null) {
      // don't know how to verify
      NucleusLogger.METADATA.warn("Unable to validate relation " + ammd.getFullFieldName());
      return;
    }
    int pkPos = pkPositions[0];
    AbstractMemberMetaData pkMemberMetaData = childAcmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);
    Class<?> pkType = pkMemberMetaData.getType();
    if (pkType.equals(Long.class) ||
        (pkType.equals(String.class) && !pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK))) {
      throw new DatastoreMetaDataException(
          childAcmd, pkMemberMetaData,
          "Cannot have a " + pkType.getName() + " primary key and be a child object "
          + "(owning field is " + ammd.getFullFieldName() + ").");
    }
  }

  private AbstractMemberMetaData validatePrimaryKey() {
    int[] pkPositions = acmd.getPKMemberPositions();
    if (pkPositions == null) {
      throw new DatastoreMetaDataException(acmd, "No primary key defined.");
    }
    if (pkPositions.length != 1) {
      throw new DatastoreMetaDataException(acmd, "More than one primary key field.");
    }
    int pkPos = pkPositions[0];
    AbstractMemberMetaData pkMemberMetaData = acmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPos);

    Class<?> pkType = pkMemberMetaData.getType();
    if (pkType.equals(Long.class)) {
      noParentAllowed = true;
    } else if (pkType.equals(String.class)) {
      if (!DatastoreManager.isEncodedPKField(acmd, pkPos)) {
        noParentAllowed = true;
      }
    } else if (pkType.equals(Key.class)) {

    } else {
      throw new DatastoreMetaDataException(
          acmd, pkMemberMetaData, "Unsupported primary key type: " + pkType.getName());
    }
    return pkMemberMetaData;
  }

  static final class DatastoreMetaDataException extends NucleusUserException {
    private static final String MSG_FORMAT_CLASS_ONLY = "Error in meta-data for %s: %s";
    private static final String MSG_FORMAT = "Error in meta-data for %s.%s: %s";

    private DatastoreMetaDataException(AbstractClassMetaData acmd, String msg) {
      super(String.format(MSG_FORMAT_CLASS_ONLY, acmd.getFullClassName(), msg));
    }

    private DatastoreMetaDataException(
        AbstractClassMetaData acmd, AbstractMemberMetaData ammd, String msg) {
      super(String.format(MSG_FORMAT, acmd.getFullClassName(), ammd.getName(), msg));
    }
  }
}
