package com.ethlo.kfka;

public interface KfkaManager
{
    void add(KfkaMessage msg);

    void clean();
    
    void clearAll();

    long findfirst(String topic, String type);

    long findLatest(String topic, String type);

    void addListener(KfkaMessageListener l);

    void addListener(KfkaMessageListener l, KfkaPredicate kfkaPredicate);

    void clearCache();

    long loadAll();
}