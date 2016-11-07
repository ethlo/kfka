package com.ethlo.kfka;

import java.util.Date;

public interface KfkaPointer
{
    void seekTo(Date timestamp);
    
    void seekTo(String msgId);
    
    boolean hasPrevious();
}