module ActiveCassandra
  
  module Indexes
    
    def save(*)
      indexies = indexies_to_update
      if super && !indexies.blank?
        update_indexies(indexies) 
      end
    end
    
    def save!(*args)
      save(*args)
    end
    
    def delete(*args)
      super(*args)
      cleanup
    end
    
    def destroy(*args)
      super(*args)
      cleanup
    end
    
    def indexies_to_update
      result = []
      self.attributes.each do |key, value|
        column = column_for_attribute(key)
        if column.index && self.changed_attributes.has_key?(key)
          result << [column.name, value, changed_attributes[key]]         
        end 
      end
      result
    end
    
    def update_indexies(indexies)    
      indexies.each do |name, new_value, old_value|
        update_index(name, new_value, old_value)
      end
    end
    
    def update_index(index_name, new_value, old_value)
      remove_index(index_name, old_value)
      add_index(index_name, new_value)
    end
    
    def remove_index(index_name, value)
      self.class.indexer.remove(index_name, value) unless value.blank?
    end
    
    def add_index(index_name, value)
      self.class.indexer.insert(index_name, value, {"#{self.id}" => ""})
    end
    
    def cleanup
      columns.each do |column|
        if column.index
          remove_index(column.name, self.attributes["#{column.name}"])
        end
      end
    end
 
  end

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
      #TODO: rewrite all this code
      if index_value.blank?
        @klass.logger.warn("Warning: index value can not be blank")    
        return nil
      end

      super_column = super_column_name(index_column)
      
      keys = @column_family.get(index_value, super_column)
      
      return nil if keys.blank?  
    end
    
    def find(index_column, index_value)
      
      if index_value.blank?
        @klass.logger.warn("Warning: index value can not be blank")    
        return nil
      end

      super_column = super_column_name(index_column)
      
      keys = @column_family.get(index_value, super_column)
      
      return nil if keys.blank?  
      
      @klass.getter.find(keys.columns.first.name)
    end
    
    def insert(index_column, index_value, attributes)
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