/**********************************************************************
Copyright (c) 2003 Erik Bengtson and others. All rights reserved. 
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
2003 Andy Jefferson - converted to use Reflection
2004 Andy Jefferson - changed to give targetException on Invocation error
    ...
**********************************************************************/
package org.datanucleus.store.mapped.mapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.util.Localiser;

/**
 * Factory class for creating Mapping instances.
 */
public final class DatastoreMappingFactory
{
    private static final Localiser LOCALISER=Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Private constructor to prevent instantiation. */
    private DatastoreMappingFactory()
    {
    }

    /** cache of constructors keyed by mapping class **/
    private static Map mappingConstructors = new HashMap();

    /** constructor arguments **/
    private static final Class[] ctr_args_classes = new Class[]{JavaTypeMapping.class,
                                                                MappedStoreManager.class, 
                                                                DatastoreField.class};

    /**
     * Get a new instance of the Mapping using the Store Manager, type and field.
     * @param mappingClass the Mapping class to be created
     * @param mapping The java mapping type
     * @param storeMgr The Store Manager
     * @param column The column to map
     * @return The Mapping
     */
    public static DatastoreMapping createMapping(Class mappingClass, JavaTypeMapping mapping, 
            MappedStoreManager storeMgr, DatastoreField column)
    {
        Object obj = null;
        try
        {
            Object[] args = new Object[]{mapping, storeMgr, column};
            Constructor ctr = (Constructor) mappingConstructors.get(mappingClass);
            if( ctr == null )
            {
                ctr = mappingClass.getConstructor(ctr_args_classes);
                mappingConstructors.put(mappingClass,ctr);
            }
            try
            {
                obj = ctr.newInstance(args);
            }
            catch (InvocationTargetException e)
            {
                throw new NucleusException(LOCALISER.msg("041009", mappingClass.getName(), 
                    e.getTargetException()), e.getTargetException()).setFatal();
            }
            catch (Exception e)
            {
                throw new NucleusException(LOCALISER.msg("041009", mappingClass.getName(), e), e).setFatal();
            }
        }
        catch (NoSuchMethodException nsme)
        {
            throw new NucleusException(LOCALISER.msg("041007", JavaTypeMapping.class, MappedStoreManager.class, 
                DatastoreField.class, mappingClass.getName())).setFatal();
        }
        return (DatastoreMapping) obj;
    }
}