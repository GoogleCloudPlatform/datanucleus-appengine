/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Category;
import com.google.appengine.api.datastore.Email;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.IMHandle;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.PhoneNumber;
import com.google.appengine.api.datastore.PostalAddress;
import com.google.appengine.api.datastore.Rating;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.users.User;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.metadata.JDOMetaDataManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ArrayMetaData;
import org.datanucleus.metadata.CollectionMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.MetaDataManager;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.Set;

import javax.jdo.spi.PersistenceCapable;

/**
 * We want all the types that are natively supported by the datastore to be in
 * the default fetch group.  This saves our users the hassle of having to
 * understand fetch groups and having to add a special attribute to persistent
 * members of these types.  Getting fields of these types into the default
 * fetch group is easy - we've added them all to our plugin.xml.  However,
 * getting Collections and Arrays of these types into the default fetch group
 * is hard.  The reason is that there is no way to tell DataNucleus to put
 * Collections of certain types in the default fetch group (List<Key> for
 * example) and to keep Collections of other types out of the default fetch
 * group (List<Shoes> for example).  Furthermore, while DataNucleus can
 * distinguish between array types (the component type is part of the class
 * itself), the array types for all the primitive types already have default
 * fetch group values of 'false' configured in the datanucleus-core plugin.xml.
 * As we've seen time and time again, there is no reliable way to ensure the
 * order in which the plugin files get loaded, so just overriding these values
 * in our own plugin.xml isn't an option.  So, in order to get around all of
 * this we install our own {@link MetaDataManager} for JDO.  This class makes
 * its own pass over the metadata for each persistence capable class and
 * adjusts the fetch group behavior of Arrays and Collections to be what we
 * want.  This is pretty fragile but for now it does the job nicely.
 *
 * @author Max Ross <max.ross@gmail.com>
 * 
 * TODO Remove this nonsense. The JDO dfg is defined in specifications. The store plugin is the
 * place to load additional fields as required by the datastore. Besides which this process
 * is not being applied for JPA (which also has a "dfg"), and it is not being applied
 * to the enhancement process either.
 */
public class DatastoreJDOMetaDataManager extends JDOMetaDataManager {

  /**
   * A {@link Set} of all classes for which containers (collections and arrays)
   * should be in the default fetch group.
   *
   * If you're adding classes to this set you best add them to plugin.xml
   * as well.
   */
  private static final Set<Class<?>> DEFAULT_FETCH_GROUP_TYPE_OVERRIDES = Utils.<Class<?>>newHashSet(
    Blob.class,
    Category.class,
    Email.class,
    GeoPt.class,
    IMHandle.class,
    Key.class,
    Link.class,
    PhoneNumber.class,
    PostalAddress.class,
    Rating.class,
    ShortBlob.class,
    Text.class,
    BlobKey.class,
    User.class,
    boolean.class,
    Boolean.class,
    byte.class,
    Byte.class,
    int.class,
    Integer.class,
    short.class,
    Short.class,
    char.class,
    Character.class,
    long.class,
    Long.class,
    double.class,
    Double.class,
    float.class,
    Float.class,
    Date.class,
    Enum.class,
    String.class
  );

  /**
   * There's no way to mutate this field from outside the class so we resort to reflection.
   */
  private static final Field JDO_FIELD_FLAG_FIELD;
  static {
    try {
      JDO_FIELD_FLAG_FIELD = AbstractMemberMetaData.class.getDeclaredField("persistenceFlags");
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    JDO_FIELD_FLAG_FIELD.setAccessible(true);
  }

  public DatastoreJDOMetaDataManager(NucleusContext ctxt) {
    super(ctxt);
  }

  @Override
  protected void populateAbstractClassMetaData(AbstractClassMetaData acmd, ClassLoaderResolver clr,
                                               ClassLoader loader) {
    super.populateAbstractClassMetaData(acmd, clr, loader);
    // We'll make our own pass over the members.
    // no accessor for 'members' so we get at one element at a time
    for (int i = 0; i < acmd.getNoOfMembers(); i++) {
      AbstractMemberMetaData ammd = acmd.getMetaDataForMemberAtRelativePosition(i);
      Class elementType = null;
      if (ammd.getContainer() instanceof CollectionMetaData) {
        elementType = clr.classForName(((CollectionMetaData) ammd.getContainer()).getElementType());
      } else if (ammd.getContainer() instanceof ArrayMetaData) {
        elementType = ammd.getType().getComponentType();
      }
      if (elementType != null &&
          !ammd.isDefaultFetchGroup() &&
          !ammd.getPersistenceModifier().equals(FieldPersistenceModifier.NONE) &&
          !ammd.getPersistenceModifier().equals(FieldPersistenceModifier.TRANSACTIONAL) &&
          shouldBeInDefaultFetchGroup(elementType)) {
        moveToDefaultFetchGroup(ammd);
      }
    }
  }

  private boolean shouldBeInDefaultFetchGroup(Class elementType) {
    if (DEFAULT_FETCH_GROUP_TYPE_OVERRIDES.contains(elementType)) {
      return true;
    }
    return !elementType.equals(Object.class) && shouldBeInDefaultFetchGroup(
        elementType.getSuperclass());
  }

  private void moveToDefaultFetchGroup(AbstractMemberMetaData ammd) {
    ammd.setDefaultFetchGroup(true);
    byte serializable = 0;
    if (Serializable.class.isAssignableFrom(ammd.getType()) || ammd.getType().isPrimitive()) {
      serializable = PersistenceCapable.SERIALIZABLE;
    }
    // bitmask logic copied from AbstractMemberMetaData.populate()

    // need to set CHECK_READ, CHECK_WRITE, and SERIALIZABLE
    byte jdoFieldFlag = (byte) (PersistenceCapable.CHECK_READ | PersistenceCapable.CHECK_WRITE | serializable);

    try {
      JDO_FIELD_FLAG_FIELD.set(ammd, jdoFieldFlag);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}