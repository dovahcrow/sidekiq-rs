require 'sidekiq'

class Error
  include Sidekiq::Worker
  sidekiq_options :queue => :analytics

  def perform()
  end
end
