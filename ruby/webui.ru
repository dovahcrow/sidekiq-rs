require 'sidekiq'
require 'redis-namespace'

Sidekiq.configure_client do |config|
  config.redis = { :size => 1, namespace: "banshee" }
end

require 'sidekiq/web'
run Sidekiq::Web
