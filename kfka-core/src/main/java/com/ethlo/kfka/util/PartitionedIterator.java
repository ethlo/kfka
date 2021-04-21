/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ethlo.kfka.util;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2018 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class PartitionedIterator<T> implements Iterator<List<T>>
{
    private Iterator<T> iterator;
    private int size;

    public PartitionedIterator(final Iterator<T> iterator, final int size)
    {
        this.iterator = iterator;
        this.size = size;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public List<T> next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }
        Object[] array = new Object[size];
        int count = 0;
        for (; count < size && iterator.hasNext(); count++)
        {
            array[count] = iterator.next();
        }
        for (int i = count; i < size; i++)
        {
            array[i] = null;
        }

        @SuppressWarnings("unchecked")
        List<T> list = Collections.unmodifiableList((List<T>) Arrays.asList(array));
        return (count == size) ? list : list.subList(0, count);
    }
}
