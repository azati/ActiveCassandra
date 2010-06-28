module ActiveCassandra
  class Indexer
    
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
    
    def super_column_name(column)
      "#{@klass.name.demodulize.underscore.pluralize}_#{column.pluralize}" 
    end
    
    def all
      result = []
      @column_family.get(:all).each do |key, columns|
        next if columns.empty?
        result << @klass.instantiate(key, attributes_from_columns(columns))
      end
      result
    end
    
    def find(index_column, index_value)
      p index_column.inspect
      p index_value.inspect
      
      super_column = super_column_name(index_column)
      
      keys = @column_family.get(index_value, super_column)
      
      return nil if keys.blank?
      
      result = []
      keys.columns.each do |key|
        result << @klass.getter.find(key.name)
      end
      #@klass.instantiate(key, attributes_from_columns(columns))
      result[0]
    end
    
    def insert(index_column, index_value, attributes)
      p index_column.inspect
      p index_value.inspect
      super_column = super_column_name(index_column)
      @column_family.insert(index_value, attributes, super_column)
    end
    
    def remove(index_column, index_value)
      @column_family.remove(index_value, super_column_name(index_column))
    end
    
    def first
      
    end
    
    def last
      
    end
    
    
  end
end