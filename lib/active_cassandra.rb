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
    autoload :ColumnFamily
    autoload :Base
    autoload :Persistence
    autoload :NativeAttribute
  end
  
  module AttributeMethods
    extend ActiveSupport::Autoload

    eager_autoload do
      autoload :Read
      autoload :Write
    end
  end
  
end
