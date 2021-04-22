package com.ethlo.kfka.sse;

/*-
 * #%L
 * kfka
 * %%
 * Copyright (C) 2017 Morten Haraldsen (ethlo)
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

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ethlo.kfka.KfkaManager;
import com.ethlo.kfka.KfkaMessage;
import com.ethlo.kfka.KfkaMessageListener;
import com.ethlo.kfka.KfkaPredicate;

@RestController
public class EventController
{
    private final static Logger logger = LoggerFactory.getLogger(EventController.class);
    // Set of current connections
    private final Set<Emitter> emitters = Collections.newSetFromMap(new ConcurrentHashMap<Emitter, Boolean>());
    @Autowired
    private KfkaManager kfkaManager;

    @RequestMapping(value = "/v1/events", method = RequestMethod.GET)
    public SseEmitter event(
            @RequestHeader(value = "Last-Event-ID", required = false) String lastEventIdHeader,
            @RequestParam(required = true, value = "topic") String topic,
            @RequestParam(required = false, value = "rewind") Integer rewind,
            @RequestParam(required = false, value = "lastEventId") Long lastEventIdOverride
    ) throws IOException
    {
        final SseEmitter sseEmitter = new SseEmitter();
        final Emitter emitter = new Emitter(sseEmitter);

        logger.debug("Opening emitter {}", emitter.hashCode());
        this.emitters.add(emitter);

        final Long lastEventId = lastEventIdOverride != null ? lastEventIdOverride : (lastEventIdHeader != null ? Long.parseLong(lastEventIdHeader) : null);

        final KfkaPredicate p = new KfkaPredicate().topic(topic);
        if (lastEventId != null)
        {
            p.lastSeenMessageId(lastEventId + 1);
            p.rewind(Integer.MIN_VALUE + 10);
        }
        else
        {
            final int offset = rewind != null ? Math.abs(rewind) * -1 : -20;
            p.rewind(offset);
        }

        final KfkaMessageListener l = kfkaManager.addListener((msg) ->
        {
            try
            {
                emitter.emit(msg);
            }
            catch (RuntimeException exc)
            {
                throw exc;
            }
            catch (IOException exc)
            {
                throw new AssertionError(exc);
            }
        }, p);

        emitter.setListener(l);

        // Remove on completion
        sseEmitter.onCompletion(new Runnable()
        {
            @Override
            public void run()
            {
                logger.debug("Emitter {} closed", emitter.hashCode());
                emitters.remove(emitter);
                kfkaManager.removeListener(emitter.getListener());
            }
        });


        return sseEmitter;
    }

    public class Emitter
    {
        private final SseEmitter emitter;
        private KfkaMessageListener listener;

        public Emitter(SseEmitter emitter)
        {
            this.emitter = emitter;
        }

        public KfkaMessageListener getListener()
        {
            return this.listener;
        }

        public void setListener(KfkaMessageListener l)
        {
            this.listener = l;
        }

        public void emit(KfkaMessage event) throws IOException
        {
            try
            {
                this.emitter.send(SseEmitter.event()
                        .id(Long.toString(event.getId()))
                        .data(event.getPayload(), MediaType.APPLICATION_JSON));
            }
            catch (IllegalStateException exc)
            {
                logger.debug(exc.getMessage());
            }
        }
    }
}
