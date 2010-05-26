module ActiveCassandra
  module Persistence
    
    def ClassMethods
     
   end
   
    
   def save
     self.class.getter.insert(self.key, self.attributes)
   end
   
   def delete
     self.class.delete(self.key)
   end
   
   alias :update :save
   
   def update_attributes(attributes)
     self.attributes = attributes
     self.save
   end
   
   def persisted?
     true
     #!(new_record? || destroyed?)
   end
    
  end
end