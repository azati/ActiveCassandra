module ActiveCassandra
  class NonRelation
    
    def initialize(klass, column_family)
      @klass = klass
      @column_family = column_family
    end
    
    def attributes_from_columns(columns)
      attributes = {}
      columns.each do |column|
        attributes[column.name] = column.value
      end
      attributes
    end
    
    def all
      result = []
      @column_family.get(:all).each do |key, columns|
        next if columns.empty?
        result << @klass.instantiate(key, attributes_from_columns(columns))
      end
      result
    end
    
    def find(key)
      columns = @column_family.get(key)
      return nil if columns.blank?
      @klass.instantiate(key, attributes_from_columns(columns))
    end
    
    def insert(key, attributes)
      @column_family.insert(key, attributes)
    end
    
    def destroy(key)
      @column_family.remove(key)
    end
    
    def first
      
    end
    
    def last
      
    end
    
    
  end
end