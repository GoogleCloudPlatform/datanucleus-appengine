/**********************************************************************
Copyright (c) 2003 Mike Martin and others. All rights reserved.
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
2003 Andy Jefferson - coding standards
2004 Andy Jefferson - implementation of newScalarExpression, newLiteral
2005 Andy Jefferson - basic serialisation support
2005 Andy Jefferson - updated serialisation using SCOUtils methods
    ...
**********************************************************************/
package org.datanucleus.store.mapped.mapping;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.scostore.MapStore;
import org.datanucleus.store.types.backed.BackedSCO;
import org.datanucleus.store.types.SCO;
import org.datanucleus.store.types.SCOContainer;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.NucleusLogger;

/**
 * SCO Mapping for Map types.
 */
public class MapMapping extends AbstractContainerMapping implements MappingCallbacks
{
    /**
     * Accessor for the Java type represented here.
     * @return The java type
     */
    public Class getJavaType()
    {
        return Map.class;
    }

    // ---------------- Implementation of MappingCallbacks --------------------

    public void insertPostProcessing(ObjectProvider op)
    {
    }

    /**
     * Method to be called after the insert of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postInsert(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        java.util.Map value = (java.util.Map) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
            }
            return;
        }

        if (value == null)
        {
            // replace null map with an empty SCO wrapper
            replaceFieldWithWrapper(ownerOP, null, false, false);
            return;
        }

        if (!mmd.isCascadePersist())
        {
            // Field doesnt support cascade-persist so no reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007006", mmd.getFullFieldName()));
            }

            // Check for any persistable keys/values that arent persistent
            ApiAdapter api = ec.getApiAdapter();
            Set entries = value.entrySet();
            Iterator iter = entries.iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry)iter.next();
                if (api.isPersistable(entry.getKey()))
                {
                    if (!api.isPersistent(entry.getKey()) && !api.isDetached(entry.getKey()))
                    {
                        // Key is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), entry.getKey());
                    }
                }
                if (api.isPersistable(entry.getValue()))
                {
                    if (!api.isPersistent(entry.getValue()) && !api.isDetached(entry.getValue()))
                    {
                        // Value is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), entry.getValue());
                    }
                }
            }
            replaceFieldWithWrapper(ownerOP, value, false, false);
        }
        else
        {
            // Reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007007", mmd.getFullFieldName()));
            }

            if (value.size() > 0)
            {
                // Add the entries direct to the datastore
                MappedStoreManager storeMgr = datastoreContainer.getStoreManager();
                ((MapStore) storeMgr.getBackingStoreForField(ownerOP.getExecutionContext().getClassLoaderResolver(), mmd, value.getClass())).putAll(ownerOP, value);

                // Create a SCO wrapper with the entries loaded
                replaceFieldWithWrapper(ownerOP, value, false, false);
            }
            else
            {
                // Create a SCO wrapper
                replaceFieldWithWrapper(ownerOP, null, false, false);
            }
        }
    }

    /**
     * Method to be called after any update of the owner class element.
     * @param ownerOP ObjectProvider of the owner
     */
    public void postUpdate(ObjectProvider ownerOP)
    {
        ExecutionContext ec = ownerOP.getExecutionContext();
        MappedStoreManager storeMgr = datastoreContainer.getStoreManager();
        java.util.Map value = (java.util.Map) ownerOP.provideField(getAbsoluteFieldNumber());
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            if (value != null)
            {
                // Make sure the keys/values are ok for proceeding
                SCOUtils.validateObjectsForWriting(ec, value.keySet());
                SCOUtils.validateObjectsForWriting(ec, value.values());
            }
            return;
        }

        if (value == null)
        {
            // replace null map with empty SCO wrapper
            replaceFieldWithWrapper(ownerOP, null, false, false);
            return;
        }

        if (value instanceof SCOContainer)
        {
            SCOContainer sco = (SCOContainer) value;

            if (ownerOP.getObject() == sco.getOwner() && mmd.getName().equals(sco.getFieldName()))
            {
                // Flush any outstanding updates
                ownerOP.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)sco).getBackingStore(), ownerOP);

                return;
            }

            if (sco.getOwner() != null)
            {
                throw new NucleusException("Owned second-class object was somehow assigned to a field other than its owner's").setFatal();
            }
        }

        if (!mmd.isCascadeUpdate())
        {
            // User doesnt want to update by reachability
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007008", mmd.getFullFieldName()));
            }
            return;
        }
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            NucleusLogger.PERSISTENCE.debug(LOCALISER.msg("007009", mmd.getFullFieldName()));
        }

        // Update the datastore with this value of map (clear old entries and add new ones)
        // This method could be called in two situations
        // 1). Update a map field of an object, so UpdateRequest is called, which calls here
        // 2). Persist a new object, and it needed to wait til the element was inserted so
        //     goes into dirty state and then flush() triggers UpdateRequest, which comes here
        MapStore store = ((MapStore) storeMgr.getBackingStoreForField(
            ec.getClassLoaderResolver(), mmd, value.getClass()));

        // TODO Consider making this more efficient picking the ones to remove/add
        // e.g use an update() method on the backing store like for CollectionStore
        store.clear(ownerOP);
        store.putAll(ownerOP, value);

        // Replace the field with a wrapper containing these entries
        replaceFieldWithWrapper(ownerOP, value, false, false);
    }

    /**
     * Method to be called before any delete of the owner class element.
     * @param sm StateManager of the owner
     **/
    public void preDelete(ObjectProvider sm)
    {
        // Do nothing - dependent deletion is performed by deleteDependent()
        if (containerIsStoredInSingleColumn())
        {
            // Do nothing when serialised since we are handled in the main request
            return;
        }

        // makes sure field is loaded
        sm.isLoaded(getAbsoluteFieldNumber());
        java.util.Map value = (java.util.Map) sm.provideField(getAbsoluteFieldNumber());
        if (value == null || value.isEmpty())
        {
            return;
        }

        if (!(value instanceof SCO))
        {
            // Make sure we have a SCO wrapper so we can clear from the datastore
            value = (java.util.Map)sm.wrapSCOField(mmd.getAbsoluteFieldNumber(), value, false, false, true);
        }
        value.clear();

        // Flush any outstanding updates
        sm.getExecutionContext().flushOperationsForBackingStore(((BackedSCO)value).getBackingStore(), sm);
    }
}
