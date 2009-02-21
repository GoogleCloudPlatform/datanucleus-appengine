// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
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
          DatastoreManager.ANCESTOR_PK);

  private static final Set<String> NOT_PRIMARY_KEY_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME,
          DatastoreManager.ANCESTOR_PK);

  private static final Set<String> REQUIRES_ENCODED_STRING_PK_EXTENSIONS =
      Utils.newHashSet(
          DatastoreManager.PK_ID,
          DatastoreManager.PK_NAME);

  public void validate(AbstractClassMetaData acmd) {
    NucleusLogger.METADATA.info(
        "Performing appengine-specific metadata validation for " + acmd.getFullClassName());
    AbstractMemberMetaData pkMemberMetaData = validatePrimaryKey(acmd);
    validateExtensions(acmd, pkMemberMetaData);
    NucleusLogger.METADATA.info(
        "Finished performing appengine-specific metadata validation for " + acmd.getFullClassName());
  }

  private void validateExtensions(
      AbstractClassMetaData acmd, AbstractMemberMetaData pkMemberMetaData) {
    Set<String> found = Utils.newHashSet();
    // can only have one field with this extension
    for (AbstractMemberMetaData ammd : acmd.getManagedMembers()) {
      for (String extension : ONE_OR_ZERO_EXTENSIONS) {
        if (ammd.hasExtension(extension)) {
          if (!found.add(extension)) {
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
    }
  }

  private AbstractMemberMetaData validatePrimaryKey(AbstractClassMetaData acmd) {
    int[] pkPositions = acmd.getPKMemberPositions();
    if (pkPositions == null) {
      throw new DatastoreMetaDataException(acmd, "No primary key defined.");
    }
    if (pkPositions.length != 1) {
      throw new DatastoreMetaDataException(acmd, "More than one primary key field.");
    }
    int pkPos = pkPositions[0];
    AbstractMemberMetaData pkMemberMetaData = acmd.getMetaDataForManagedMemberAtPosition(pkPos);

    Class<?> pkType = pkMemberMetaData.getType();
    if (pkType.equals(Long.class)) {
      AbstractMemberMetaData ammd = hasFieldWithExtension(acmd, DatastoreManager.ANCESTOR_PK);
      if (ammd != null) {
        throw new DatastoreMetaDataException(
            acmd, ammd, "Cannot have a Long primary key and an ancestor field.");
      }
      // TODO(maxr) make sure there is no parent key provider
    } else if (pkType.equals(String.class)) {
      if (!DatastoreManager.isEncodedPKField(acmd, pkPos)) {
        AbstractMemberMetaData ammd = hasFieldWithExtension(acmd, DatastoreManager.ANCESTOR_PK);
        if (ammd != null) {
          throw new DatastoreMetaDataException(acmd, ammd,
              "Cannot have an unencoded String primary key and an ancestor field.");
        }
        // TODO(maxr) make sure there is no parent key provider
      }
    } else if (pkType.equals(Key.class)) {

    } else {
      throw new DatastoreMetaDataException(
          acmd, pkMemberMetaData, "Unsupported primary key type: " + pkType.getName());
    }
    return pkMemberMetaData;
  }

  private AbstractMemberMetaData hasFieldWithExtension(AbstractClassMetaData acmd, String extension) {
    for (AbstractMemberMetaData ammd : acmd.getManagedMembers()) {
      if (ammd.hasExtension(extension)) {
        return ammd;
      }
    }
    return null;
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
