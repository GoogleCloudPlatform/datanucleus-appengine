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
import org.datanucleus.metadata.IdentityStrategy;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.OrderMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.metadata.SequenceMetaData;
import org.datanucleus.util.NucleusLogger;

import java.util.Map;
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

  /**
   * Defines the various actions we can take when we encounter ignorable meta-data.
   */
  enum IgnorableMetaDataBehavior {
    NONE, // Do nothing at all.
    WARN, // Log a warning.
    ERROR;// Throw an exception.

    private static IgnorableMetaDataBehavior valueOf(String val, IgnorableMetaDataBehavior returnIfNull) {
      if (val == null) {
        return returnIfNull;
      }
      return valueOf(val);
    }
  }

  /**
   * Config property that determines the action we take when we encounter
   * ignorable meta-data.
   */
  private static final String
      IGNORABLE_META_DATA_BEHAVIOR_PROPERTY = "datanucleus.appengine.ignorableMetaDataBehavior";

  /**
   * This message is appended to every ignorable meta-data warning so users
   * know they can configure it.
   */
  static final String ADJUST_WARNING_MSG =
      String.format("You can modify this warning by setting the %s property in your config.  "
                    + "A value of %s will silence the warning.  "
                    + "A value of %s will turn the warning into an exception.",
                    IGNORABLE_META_DATA_BEHAVIOR_PROPERTY,
                    IgnorableMetaDataBehavior.NONE,
                    IgnorableMetaDataBehavior.ERROR);

  private static final Set<Integer> NON_REPEATABLE_RELATION_TYPES = Utils.newHashSet(
      Relation.ONE_TO_MANY_BI,
      Relation.ONE_TO_MANY_UNI,
      Relation.ONE_TO_ONE_BI,
      Relation.ONE_TO_ONE_UNI
  );

  private static final String ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE =
      "datanucleus.appengine.allowMultipleRelationsOfSameType";

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
    validateFields(pkMemberMetaData);
    validateClass();
    NucleusLogger.METADATA.info(
        "Finished performing appengine-specific metadata validation for " + acmd.getFullClassName());
  }

  private void validateClass() {
    // Look for uniqueness constraints.  Not supported but not necessarily an error
    if (acmd.getUniqueMetaData() != null && acmd.getUniqueMetaData().length > 0) {
      handleIgnorableMapping("The datastore does not support uniqueness constraints.", 
                             "The constraint definition will be ignored.");
    }
  }

  private void validateFields(AbstractMemberMetaData pkMemberMetaData) {
    Set<String> foundOneOrZeroExtensions = Utils.newHashSet();
    Map<Class<?>, String> nonRepeatableRelationTypes = Utils.newHashMap();
    Class<?> pkClass = pkMemberMetaData.getType();

    // the constraints that we check across all fields apply to the entire
    // persistent class hierarchy so we're going to validate every field
    // at every level of the hierarchy.  As an example, this lets us detect
    // multiple one-to-many relationships at different levels of the class
    // hierarchy
    AbstractClassMetaData curCmd = acmd;
    do {
      for (AbstractMemberMetaData ammd : curCmd.getManagedMembers()) {
        validateField(pkMemberMetaData, pkClass, foundOneOrZeroExtensions, nonRepeatableRelationTypes, ammd);
      }
      curCmd = curCmd.getSuperAbstractClassMetaData();
    } while (curCmd != null);
  }

  private void validateField(AbstractMemberMetaData pkMemberMetaData,
                             Class<?> pkClass, Set<String> foundOneOrZeroExtensions,
                             Map<Class<?>, String> nonRepeatableRelationTypes, AbstractMemberMetaData ammd) {
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

    checkForIllegalChildField(ammd);

    if (ammd.getRelationType(clr) != Relation.NONE) {
      // Look for "eager" relationships.  Not supported but not necessarily an error
      // since we can always fall back to "lazy."
      if (ammd.isDefaultFetchGroup() && !ammd.isEmbedded()) {
        // We have separate error messages for JPA vs JDO because eagerness is configured
        // differently between the two.
        String msg = isJPA() ?
                     "The datastore does not support joins and therefore cannot honor requests to eagerly load related objects." :
                     "The datastore does not support joins and therefore cannot honor requests to place related objects in the default fetch group.";
        handleIgnorableMapping(ammd, msg, "The field will be fetched lazily on first access.");
      }

      // We don't support many-to-many at all
      if (ammd.getRelationType(clr) == Relation.MANY_TO_MANY_BI) {
        throw new DatastoreMetaDataException(
            acmd, ammd, "Many-to-many is not currently supported in App Engine.  As a workaround, "
                        + "consider maintaining a List<Key> on both sides of the relationship.  "
                        + "See http://code.google.com/appengine/docs/java/datastore/relationships.html#Unowned_Relationships "
                        + "for more information.");
      } else if (ammd.getEmbeddedMetaData() == null &&
                 NON_REPEATABLE_RELATION_TYPES.contains(ammd.getRelationType(clr)) &&
                 !getBooleanConfigProperty(ALLOW_MULTIPLE_RELATIONS_OF_SAME_TYPE)) {
        Class<?> relationClass;
        if (ammd.getCollection() != null) {
          relationClass = clr.classForName(ammd.getCollection().getElementType());
        } else if (ammd.getArray() != null) {
          relationClass = clr.classForName(ammd.getArray().getElementType());
        } else {
          relationClass = clr.classForName(ammd.getTypeName());
        }

        // Add the actual type of the field to the list of types that can't
        // repeat.  If that type was already present, problem.
        for (Class<?> existingRelationClass : nonRepeatableRelationTypes.keySet()) {
          if (existingRelationClass.isAssignableFrom(relationClass) ||
              relationClass.isAssignableFrom(existingRelationClass)) {
            throw newMultipleRelationshipFieldsOfSameType(
                ammd, relationClass, ammd.getName(), nonRepeatableRelationTypes.get(existingRelationClass));
          }
        }
        nonRepeatableRelationTypes.put(relationClass, ammd.getName());
      }
    }

    if (ammd.getValueGeneratorName() != null) {
      SequenceMetaData sequenceMetaData = metaDataManager.getMetaDataForSequence(clr, ammd.getValueGeneratorName());
      if (sequenceMetaData != null && sequenceMetaData.getInitialValue() != 1) {
        handleIgnorableMapping(
            ammd, "The datastore does not support the configuration of initial sequence values.",
            "The first value for this sequence will be 1.");
      }
    }
  }

  private DatastoreMetaDataException newMultipleRelationshipFieldsOfSameType(
      AbstractMemberMetaData ammd, Class<?> relationClass, String field1, String field2) {
    return new DatastoreMetaDataException(
        acmd, ammd, "Class " + acmd.getFullClassName() + " has multiple relationship fields of type "
                    + relationClass.getName() + ": " + field1 + " and " + field2 + ".  This is not yet supported.");
  }

  boolean isJPA() {
    return DatastoreManager.isJPA(metaDataManager.getOMFContext());
  }

  private boolean getBooleanConfigProperty(String configProperty) {
    return metaDataManager.getOMFContext().getPersistenceConfiguration()
        .getBooleanProperty(configProperty);
  }

  private IgnorableMetaDataBehavior getIgnorableMetaDataBehavior() {
    return IgnorableMetaDataBehavior.valueOf(
        metaDataManager.getOMFContext().getPersistenceConfiguration()
            .getStringProperty(IGNORABLE_META_DATA_BEHAVIOR_PROPERTY), IgnorableMetaDataBehavior.WARN);
  }

  void handleIgnorableMapping(String msg, String warningOnlyMsg) {
    handleIgnorableMapping(null, msg, warningOnlyMsg);
  }

  void handleIgnorableMapping(AbstractMemberMetaData ammd, String msg, String warningOnlyMsg) {
    switch (getIgnorableMetaDataBehavior()) {
      case WARN:
        if (ammd == null) {
          warn(String.format(
              "Meta-data warning for %s: %s  %s  %s",
              acmd.getFullClassName(), msg, warningOnlyMsg, ADJUST_WARNING_MSG));          
        } else {
          warn(String.format(
              "Meta-data warning for %s.%s: %s  %s  %s",
              acmd.getFullClassName(), ammd.getName(), msg, warningOnlyMsg, ADJUST_WARNING_MSG));
        }
        break;
      case ERROR:
        if (ammd == null) {
          throw new DatastoreMetaDataException(acmd, msg);
        }
        throw new DatastoreMetaDataException(acmd, ammd, msg);
      // We swallow both null and NONE
    }
  }

  // broken out for testing
  void warn(String msg) {
    NucleusLogger.METADATA.warn(msg);
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
      if (ammd.getOrderMetaData() != null) {
        verifyOneToManyOrderBy(ammd, childAcmd);
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
    if (noParentAllowed && (pkType.equals(Long.class) ||
        (pkType.equals(String.class) && !pkMemberMetaData.hasExtension(DatastoreManager.ENCODED_PK)))) {
      throw new DatastoreMetaDataException(
          childAcmd, pkMemberMetaData,
          "Cannot have a " + pkType.getName() + " primary key and be a child object "
          + "(owning field is " + ammd.getFullFieldName() + ").");
    }
  }

  private void verifyOneToManyOrderBy(AbstractMemberMetaData ammd, AbstractClassMetaData childAcmd) {
    OrderMetaData omd = ammd.getOrderMetaData();
    OrderMetaData.FieldOrder[] fieldOrders = omd.getFieldOrders();
    if (fieldOrders == null) {
      return;
    }
    for (OrderMetaData.FieldOrder fieldOrder : omd.getFieldOrders()) {
      String propertyName = fieldOrder.getFieldName();
      AbstractMemberMetaData orderField = childAcmd.getMetaDataForMember(propertyName);
      if (orderField == null) {
        // shouldn't happen since DataNuc does the same check in omd.getFieldOrders()
        throw new DatastoreMetaDataException(
            acmd, ammd,
            "Order property " + propertyName + " could not be founcd on " + childAcmd.getFullClassName());
      }

      if (orderField.hasExtension(DatastoreManager.PK_ID) ||
          orderField.hasExtension(DatastoreManager.PK_NAME)) {
        throw new DatastoreMetaDataException(
            acmd, ammd,
            "Order property " + propertyName + " is a sub-component of the primary key.  The "
            + "datastore does not support sorting by primary key components, only the "
            + "entire primary key.");
      }
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
      } else {
        // encoded string pk
        if (hasIdentityStrategy(IdentityStrategy.SEQUENCE, pkMemberMetaData)) {
          throw new DatastoreMetaDataException(
              acmd, pkMemberMetaData,
              "IdentityStrategy SEQUENCE is not supported on encoded String primary keys.");
        }
      }
    } else if (pkType.equals(Key.class)) {
      if (hasIdentityStrategy(IdentityStrategy.SEQUENCE, pkMemberMetaData)) {
        throw new DatastoreMetaDataException(
            acmd, pkMemberMetaData,
            "IdentityStrategy SEQUENCE is not supported on primary keys of type " + Key.class.getName());
      }
    } else {
      throw new DatastoreMetaDataException(
          acmd, pkMemberMetaData, "Unsupported primary key type: " + pkType.getName());
    }
    return pkMemberMetaData;
  }

  private static boolean hasIdentityStrategy(IdentityStrategy strat, AbstractMemberMetaData ammd) {
    return ammd.getValueStrategy() != null && ammd.getValueStrategy().equals(strat);
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

    @Override
    public boolean isFatal() {
      // Always fatal
      return true;
    }
  }
}
