module ActiveCassandra
  module Persistence
    
    def all
      connection.get_range(table_name).map! {|row| instantiate(connection.get(table_name, row.key))}
    end
    
    def table_name
      self.name.demodulize.underscore.pluralize
    end
    
  end
end