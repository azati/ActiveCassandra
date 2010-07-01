module ActiveCassandra
  class SuperColumnFamily
    
    def initialize(column_family_name, connection)
      @column_family_name = column_family_name
      @connection = connection
    end
   
    def get(keys, super_column = nil)
      if keys == :all
        @connection[''..''].get(@column_family_name, super_column)
      else 
        @connection[keys].get(@column_family_name, super_column)
      end
    end
    
    def insert(key, attributes, super_column = nil)     
      @connection[key].insert(@column_family_name, super_column, attributes, Time.now) 
      true  
    end
    
    def remove(key, super_column = nil)
      @connection[key].remove(@column_family_name, super_column, nil, Time.now)
      true
    end
    
    #def method_missing(method_name, *args)
    #  @connection.send(method_name, @column_family_name, *args)
    #end
    
  end
end