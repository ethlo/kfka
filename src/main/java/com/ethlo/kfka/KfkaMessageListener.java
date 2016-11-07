package com.ethlo.kfka;

public interface KfkaMessageListener
{
    void onMessage(KfkaMessage msg);    
}