module ActiveCassandra
  module Persistence
    
    def ClassMethods
     
    end
    
    def save(*args)
      
      attributes_to_insert = {}
      
      attributes.each do |key, value|
        
        attributes_to_insert[key] = value.to_s
        column = column_for_attribute(key)
        p column.inspect
        if column.index && self.changed_attributes.has_key?(key)
          puts "updating index"
          update_index(column.name, value, changed_attributes[key])
        end
        
      end

      self.class.getter.insert(self.key, attributes_to_insert)
    end
    
    def update_index(index_name, new_value, old_value)
      #self.class.indexer.remove(index_name, old_value)
      self.class.indexer.insert(index_name, new_value, {"#{self.id}" => ""})
    end
   
    def destroy
      self.class.destroy(self.key)
      @destroyed = true
    end
   
    alias :update :save
    
    def update_attributes(attributes)
      self.attributes = attributes
      self.save
    end
   
    def persisted?
      !(new_record? || destroyed?)
    end
   
    def new_record?
      @new_record
    end

    # Returns true if this object has been destroyed, otherwise returns false.
    def destroyed?
      @destroyed
    end
    
    def readonly?
      @readonly
    end
    
  end
end