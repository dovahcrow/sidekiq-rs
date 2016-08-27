require 'sidekiq'
require 'redis-namespace'

Sidekiq.configure_client do |config|
  config.redis = { namespace: 'annie' }
end

require_relative 'lib/worker/dummy'
require_relative 'lib/worker/printer'
require_relative 'lib/worker/error'



10_0.times do
  Dummy.perform_async('Stranger')
end

# 10_0.times do
#   Error.perform_async('Stranger')
# end
# 10.times do
#   Excep.perform_async("damn")
# end