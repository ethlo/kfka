package com.ethlo.kfka;

import com.hazelcast.core.MapStore;

public interface KfkaMapStore extends MapStore<Long, KfkaMessage>
{

}
