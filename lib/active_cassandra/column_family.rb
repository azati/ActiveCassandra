module ActiveCassandra
  class ColumnFamily
    
    def initialize(column_family_name, connection)
      @column_family_name = column_family_name
      @connection = connection
    end
    
    def method_missing(method_name, *args)
      @connection.send(method_name, @column_family_name, *args)
    end
    
  end
end