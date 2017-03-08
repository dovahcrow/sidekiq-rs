require 'sidekiq'

# Sidekiq.configure_client do |config|
#   config.redis = { namespace: 'annie' }
# end

require_relative 'lib/worker/panic'
require_relative 'lib/worker/printer'
require_relative 'lib/worker/error'



# 10.times do
#   Panic.perform_async('Big')
# end

10.times do
  Error.perform_async('Stranger')
end

# 10.times do
#   Printer.perform_async("Damn")
# end