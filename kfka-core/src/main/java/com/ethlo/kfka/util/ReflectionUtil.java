package com.ethlo.kfka.util;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2021 Morten Haraldsen (ethlo)
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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ethlo.kfka.KfkaMessage;

public class ReflectionUtil
{
    private ReflectionUtil()
    {
    }

    private static final Map<Class<?>, Map<String, Field>> fieldCache = new ConcurrentHashMap<>();

    public static Map<String, Field> getFields(Class<? extends Serializable> type)
    {
        return fieldCache.computeIfAbsent(type, ReflectionUtil::getAllFields);
    }

    private static Map<String, Field> getAllFields(Class<?> type)
    {
        final Map<String, Field> fields = new HashMap<>();
        for (Class<?> c = type; c != null; c = c.getSuperclass())
        {
            final Field[] declared = c.getDeclaredFields();
            for (Field f : declared)
            {
                f.setAccessible(true);
                fields.put(f.getName(), f);
            }
        }
        return fields;
    }

    public static Object getPropertyValue(KfkaMessage value, String propName)
    {
        final Field field = getFields(value.getClass()).get(propName);
        if (field != null)
        {
            try
            {
                return field.get(value);
            }
            catch (SecurityException | IllegalArgumentException | IllegalAccessException exc)
            {
                return null;
            }
        }
        return null;
    }
}
