module ActiveCassandra
  module Persistence
    
    def ClassMethods
     
    end
   
    
    def save
      self.class.getter.insert(self.key, self.attributes)
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
    
  end
end