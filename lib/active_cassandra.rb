require 'active_support/i18n'
#require 'active_cassandra/base'


module ActiveCassandra
  extend ActiveSupport::Autoload
  
  eager_autoload do
    autoload :Base
    autoload :Persistance
    autoload :AttributeMethods
  end
  
end
