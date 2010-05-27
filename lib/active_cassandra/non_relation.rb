module ActiveCassandra
  class NonRelation
    
    def initialize(klass, column_family)
      @klass = klass
      @column_family = column_family
    end
    
    def all
      result = []
      @column_family.get_range.each do |row| 
        result << @klass.instantiate(row.key, @column_family.get(row.key)) unless row.columns.blank?
      end
      result
    end
    
    def find(key)
      attributes = @column_family.get(key)
      @klass.instantiate(key, attributes)
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