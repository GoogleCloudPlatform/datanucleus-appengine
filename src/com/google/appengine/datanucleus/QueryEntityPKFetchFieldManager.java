/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
package com.google.appengine.datanucleus;

import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

/**
 * Simple implementation of a field manager for fetching values out of queries.
 * This version simply retrieves PK fields, so we can get hold of the identity.
 * FetchFieldManager really ought to be rewritten to follow this style rather than having
 * stacks of FetchState or whatever.
 */
public class QueryEntityPKFetchFieldManager extends AbstractFieldManager {

    /** Metadata for the candidate class. */
    AbstractClassMetaData cmd;

    Entity datastoreEntity;

    public QueryEntityPKFetchFieldManager(AbstractClassMetaData cmd, Entity entity) {
      this.cmd = cmd;
      this.datastoreEntity = entity;
    }

    public boolean fetchBooleanField(int fieldNumber) {
      return (Boolean) fetchObjectField(fieldNumber);
    }

    public byte fetchByteField(int fieldNumber) {
      return (Byte) fetchObjectField(fieldNumber);
    }

    public char fetchCharField(int fieldNumber) {
      return (Character) fetchObjectField(fieldNumber);
    }

    public double fetchDoubleField(int fieldNumber) {
      return (Double) fetchObjectField(fieldNumber);
    }

    public float fetchFloatField(int fieldNumber) {
      return (Float) fetchObjectField(fieldNumber);
    }

    public int fetchIntField(int fieldNumber) {
      return (Integer) fetchObjectField(fieldNumber);
    }

    public long fetchLongField(int fieldNumber) {
      return (Long) fetchObjectField(fieldNumber);
    }

    public short fetchShortField(int fieldNumber) {
      return (Short) fetchObjectField(fieldNumber);
    }

    public String fetchStringField(int fieldNumber) {
      if (isPK(fieldNumber)) {
        return fetchStringPKField(fieldNumber);
      }
      return null;
    }

    public Object fetchObjectField(int fieldNumber) {
      AbstractMemberMetaData ammd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

      if (isPK(fieldNumber)) {
        if (ammd.getType().equals(Key.class)) {
          // If this is a pk field, transform the Key into its String representation.
          return datastoreEntity.getKey();
        } else if (ammd.getType().equals(Long.class)) {
          return datastoreEntity.getKey().getId();
        }
      }
      return null;
    }

    private String fetchStringPKField(int fieldNumber) {
      if (DatastoreManager.isEncodedPKField(cmd, fieldNumber)) {
        // If this is an encoded pk field, transform the Key into its String representation.
        return KeyFactory.keyToString(datastoreEntity.getKey());
      } else {
        if (datastoreEntity.getKey().isComplete() && datastoreEntity.getKey().getName() == null) {
          // This is trouble, probably an incorrect mapping.
          throw new NucleusFatalUserException(
              "The primary key for " + cmd.getFullClassName() + " is an unencoded "
              + "string but the key of the corresponding entity in the datastore does not have a "
              + "name.  You may want to either change the primary key to be an encoded string "
              + "(add the \"" + DatastoreManager.ENCODED_PK + "\" extension), change the "
              + "primary key to be of type " + Key.class.getName() + ", or, if you're certain that "
              + "this class will never have a parent, change the primary key to be of type Long.");
        }
        return datastoreEntity.getKey().getName();
      }
    }

    protected boolean isPK(int fieldNumber) {
      int[] pkPositions = cmd.getPKMemberPositions();
      return pkPositions != null && pkPositions[0] == fieldNumber;
    }
}
