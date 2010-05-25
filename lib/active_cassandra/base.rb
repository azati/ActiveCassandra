require 'active_cassandra/persistence'

module ActiveCassandra
  class Base
    #include AttributeMethods
    
    cattr_accessor :configurations, :instance_writer => false
    @@configurations = {}
    
    class_inheritable_accessor :connection
    
    class << self
      
      include Persistence
      
      def establish_connection(spec)
        config = configurations[spec.to_s].symbolize_keys
        host     = (config[:host] || 'localhost') 
        port     = (config[:port] || '9160')
        username = config[:username] ? config[:username].to_s : 'root'
        password = config[:password].to_s
        keyspace = config[:keyspace]
  
        # Require the MySQL driver and define Mysql::Result.all_hashes
        unless defined? Cassandra
          begin
            require_library_or_gem 'cassandra'
          rescue LoadError
            $stderr.puts '!!! Please install the cassandra gem and try again: gem install cassandra.'
            raise
          end
        end

      
        self.connection = Cassandra.new(keyspace, ["#{host}:#{port}"])
      end
     
      def instantiate(row)
        object = self.allocate
  
        object.instance_variable_set(:'@attributes', row)
        object.instance_variable_set(:'@attributes_cache', {})
        object.instance_variable_set(:@new_record, false)
        object.instance_variable_set(:@readonly, false)
        object.instance_variable_set(:@destroyed, false)
        object.instance_variable_set(:@marked_for_destruction, false)
        object.instance_variable_set(:@previously_changed, {})
        object.instance_variable_set(:@changed_attributes, {})
  
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
     
      #include Persistence
      
      include AttributeMethods
    
      #include ActiveModel::AttributeMethodMatcher
       
   end
   
        
  
        
      
  attr_accessor :attributes
  
  #Base.class_eval do
    include ActiveModel::AttributeMethods
    #include ActiveModel::AttributeMethods::ClassMethods
    include AttributeMethods::Read
  #end
    
  end
  
  #Base.class_eval do
  #  include ActiveCassandra::Persistence
  #end
  
end

ActiveSupport.run_load_hooks(:active_cassandra, ActiveCassandra::Base)


