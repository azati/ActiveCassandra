module ActiveCassandra
  class ColumnFamily
    
    def initialize(column_family_name, connection)
      @column_family_name = column_family_name
      @connection = connection
    end
   
    def get(keys)
      if keys == :all
        @connection[''..''].get(@column_family_name, nil)
      else 
        @connection[keys].get(@column_family_name, nil)
      end
    end
    
    def insert(key, attributes)
      @connection[key].insert(@column_family_name, nil, attributes, Time.now) 
      true  
    end
    
    def remove(key)
      @connection[key].remove(@column_family_name, nil, nil, Time.now)
      true
    end
    
    #def method_missing(method_name, *args)
    #  @connection.send(method_name, @column_family_name, *args)
    #end
    
  end
end