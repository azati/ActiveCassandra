require 'active_support/i18n'
require 'active_support'
require 'active_support/i18n'
require 'active_model'
#require 'active_cassandra/base'


module ActiveCassandra
  extend ActiveSupport::Autoload
  
  eager_autoload do
    autoload :AttributeMethods
    autoload :NonRelation
    autoload :Indexes
    autoload :ColumnFamily
    autoload :SuperColumnFamily
    autoload :Base
    autoload :Persistence
    autoload :Columns
    autoload :Validations
    autoload :Callbacks
    autoload :DynamicFinderMatch
    autoload :Errors
  end
  
  module AttributeMethods
    extend ActiveSupport::Autoload

    eager_autoload do
      autoload :Read
      autoload :Write
      autoload :PrimaryKey
      autoload :Dirty
    end
  end

  module Validations
    extend ActiveSupport::Autoload

    eager_autoload do
      autoload :Associated
      autoload :Uniqueness
    end
  end
  
end

