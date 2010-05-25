module ActiveCassandra
  module AttributeMethods
    include ActiveModel::AttributeMethods

 
 
=begin    
    def method_missing(method_id, *args, &block)
      if @attributes.has_key?(method_id.to_s)
        return @attributes[method_id.to_s]
      end
      super
    end
=end

    
  end
end