package com.ethlo.kfka;

import java.util.function.Supplier;

@FunctionalInterface
public interface MessageIdGenerator extends Supplier<String>
{

}
