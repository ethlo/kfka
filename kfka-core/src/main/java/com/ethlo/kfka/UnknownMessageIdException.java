package com.ethlo.kfka;

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

public class UnknownMessageIdException extends RuntimeException
{
    private final String messageId;

    public UnknownMessageIdException(final String messageId)
    {
        super("Unknown message id: " + messageId);
        this.messageId = messageId;
    }

    public String getMessageId()
    {
        return messageId;
    }
}
