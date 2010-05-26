module ActiveCassandra
  module AttributeMethods
    module Read
      extend ActiveSupport::Concern
      
      included do
        attribute_method_suffix ""   
        # Undefine id so it can be used as an attribute name
        undef_method(:id) if method_defined?(:id)
      end
      
      def read_attribute(attribute_name)
        @attributes[attribute_name.to_s]
      end
     
      private
        def attribute(attribute_name)
          read_attribute(attribute_name)
        end
     
    end
  end
end