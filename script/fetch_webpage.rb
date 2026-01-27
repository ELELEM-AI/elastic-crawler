# Usage:
#   from repo root (if script is executable and has the right shebang):
#   ruby script/fetch_webpage.rb https://www.elastic.co

require_relative '../lib/crawler/http_client'
require_relative '../lib/crawler/data/url'

if ARGV.length != 1
  puts "Usage: ruby script/fetch_webpage.rb <URL>"
  exit 1
end

url_str = ARGV.first

begin
  url = Crawler::Data::URL.parse(url_str)
  client = Crawler::HttpClient.new

  response = client.get(url)
  puts "Status: #{response.code}"
  puts "Headers:"
  response.headers.each { |k, v| puts "  #{k}: #{v}" }
  puts "\nBody preview:"
  puts response.body[0, 500] # Print first 500 chars
rescue => e
  puts "Error fetching URL: #{e.class}: #{e.message}"
  puts e.backtrace.take(5)
  exit 2
end
