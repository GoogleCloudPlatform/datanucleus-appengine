/**********************************************************************
Copyright (c) 2002 Mike Martin and others. All rights reserved.
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
    Andy Jefferson - coding standards
    ...
**********************************************************************/
package org.datanucleus.store.mapped.exceptions;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.util.Localiser;

/**
 * A <tt>IncompatibleQueryElementTypeException</tt> is thrown if a variable used
 * in a query is detected to have an type incompatible with the element type of
 * the collection to which it is being applied.
 *
 * @see org.datanucleus.store.query.Query
 * @see org.datanucleus.store.scostore.CollectionStore
 */
public class IncompatibleQueryElementTypeException extends NucleusUserException
{
    private static final Localiser LOCALISER=Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /**
     * Constructs an incompatible query element type exception.
     * @param expectedType The expected element base type.
     * @param actualType The actual element type.
     */
    public IncompatibleQueryElementTypeException(String expectedType, String actualType)
    {
        super(LOCALISER.msg("021000",actualType,expectedType));
    }
}
