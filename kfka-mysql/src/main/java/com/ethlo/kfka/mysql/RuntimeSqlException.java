package com.ethlo.kfka.mysql;

import java.sql.SQLException;

public class RuntimeSqlException extends RuntimeException
{
    public RuntimeSqlException(SQLException cause)
    {
        super(cause);
    }
}
