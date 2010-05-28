module ActiveCassandra
  class Base
    #include AttributeMethods
    
    cattr_accessor :configurations, :instance_writer => false
    @@configurations = {}
    
    class_inheritable_accessor :connection
    
    class << self
     
      delegate :all, :first, :last, :to => :getter
      delegate :find, :to => :getter
      delegate :destroy, :to => :getter
      
      def establish_connection(spec)
        config = configurations[spec.to_s].symbolize_keys
        host     = (config[:host] || 'localhost') 
        port     = (config[:port] || '9160')
        username = config[:username] ? config[:username].to_s : 'root'
        password = config[:password].to_s
        keyspace = config[:keyspace]
  
        # Require the MySQL driver and define Mysql::Result.all_hashes
        unless defined? CassandraRuby
          begin
            require_library_or_gem 'thrift'
            require_library_or_gem 'cassandra_ruby'
          rescue LoadError
            $stderr.puts '!!! Please install the cassandra gem and try again: gem install cassandra.'
            raise
          end
        end

      
        self.connection = CassandraRuby::Keyspace.new(CassandraRuby::Cassandra.new(host), keyspace)
      end
     
      def instantiate(key, row)
        object = self.allocate
  
        object.instance_variable_set(:'@attributes', row)
        object.instance_variable_set(:'@attributes_cache', {})
        object.instance_variable_set(:@new_record, false)
        object.instance_variable_set(:@readonly, false)
        object.instance_variable_set(:@destroyed, false)
        object.instance_variable_set(:@marked_for_destruction, false)
        object.instance_variable_set(:@previously_changed, {})
        object.instance_variable_set(:@changed_attributes, {})
        object.instance_variable_set(:@key, key)
  
        #object.send(:_run_find_callbacks)
        #object.send(:_run_initialize_callbacks)
  
        object
      end
      
      def find_sti_class(type_name)
        if type_name.blank? || !columns_hash.include?(inheritance_column)
          self
        else
          begin
            compute_type(type_name)
          rescue NameError
            raise SubclassNotFound,
              "The single-table inheritance mechanism failed to locate the subclass: '#{type_name}'. " +
              "This error is raised because the column '#{inheritance_column}' is reserved for storing the class in case of inheritance. " +
              "Please rename this column if you didn't intend it to be used for storing the inheritance class " +
              "or overwrite #{name}.inheritance_column to use another column for that information."
          end
        end
      end
      
      def getter
        @getter ||= NonRelation.new(self, column_family)
      end
      
      def column_family
        ColumnFamily.new(column_family_name, connection)
      end
      
      def column_family_name
        self.name.demodulize.underscore.pluralize
      end
     
      def create(attributes = nil, &block)
        if attributes.is_a?(Array)
          attributes.collect { |attr| create(attr, &block) }
        else
          object = new(attributes)
          yield(object) if block_given?
          object.save
          object
        end
      end
     
      
      #include Persistence
      
      
    
      #include ActiveModel::AttributeMethodMatcher
       
   end
   
   def initialize(attributes = nil, &block)
     @attributes = {}
     
     self.class.native_attributes.each do |attr|
       options = attr[:options]
       @attributes[attr[:name]] = options[:default].blank? ? attr[:type].new : options[:default] 
     end
     
     @attributes_cache = {}
     @new_record = true
     @readonly = false
     @destroyed = false
     @marked_for_destruction = false
     @previously_changed = {}
     @changed_attributes = {}
     ## USE GUID HERE
     @key = rand(10000).to_s
     
     self.attributes = attributes unless attributes.nil?

     result = yield self if block_given?
     #_run_initialize_callbacks
     result
     
   end
   
   def attributes=(new_attributes, guard_protected_attributes = true)
     return if new_attributes.nil?
     
     attributes = new_attributes.stringify_keys 

     @attributes = attributes
     #multi_parameter_attributes = []
     #attributes = remove_attributes_protected_from_mass_assignment(attributes) if guard_protected_attributes

     #attributes.each do |k, v|
     #  if k.include?("(")
     #    multi_parameter_attributes << [ k, v ]
     #  else
     #    respond_to?(:"#{k}=") ? send(:"#{k}=", v) : raise(UnknownAttributeError, "unknown attribute: #{k}")
     #  end
     #end

     #assign_multiparameter_attributes(multi_parameter_attributes)
   end

  # Returns a hash of all the attributes with their names as keys and the values of the attributes as values.
   def attributes
     attrs = {}
     attribute_names.each { |name| attrs[name] = read_attribute(name) }
     attrs
   end
   
   def attribute_names
     @attributes.keys.sort
   end
   
   def key
     @key
   end
   
   def to_key
     [ @key ]
   end
   
   
   
        
  #Base.class_eval do
  #  
  #end
        
   
  
  #Base.class_eval do
    
    #include ActiveModel::AttributeMethods::ClassMethods
    
  #end
    
  end
  
  Base.class_eval do
    
    
    
    include ActiveCassandra::Persistence
    
    include ActiveModel::AttributeMethods
    include ActiveModel::Validations
    include ActiveModel::Conversion
    
    include AttributeMethods
    
    include AttributeMethods::Read, AttributeMethods::Write
    extend ActiveModel::Naming
    
    extend NativeAttribute
    
    #include 
  end
  
end

ActiveSupport.run_load_hooks(:active_cassandra, ActiveCassandra::Base)


