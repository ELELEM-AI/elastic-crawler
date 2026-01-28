#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

# frozen_string_literal: true

RSpec.describe(Crawler::Data::SeenUrls) do
  let(:seen_urls) { described_class.new }
  let(:url1) { Crawler::Data::URL.parse('http://example.com/page1') }
  let(:url2) { Crawler::Data::URL.parse('http://example.com/page2') }
  let(:url3) { Crawler::Data::URL.parse('http://example.com/sitemap.xml') }

  describe '#add?' do
    it 'returns true for a new URL' do
      expect(seen_urls.add?(url1)).to be(true)
    end

    it 'returns false for a duplicate URL' do
      seen_urls.add?(url1)
      expect(seen_urls.add?(url1)).to be(false)
    end

    it 'tracks content URLs separately when type is provided' do
      expect(seen_urls.add?(url1, type: :content)).to be(true)
      expect(seen_urls.content_count).to eq(1)
      expect(seen_urls.count).to eq(1)
    end

    it 'does not increment content count for sitemap URLs' do
      expect(seen_urls.add?(url3, type: :sitemap)).to be(true)
      expect(seen_urls.content_count).to eq(0)
      expect(seen_urls.count).to eq(1)
    end

    it 'tracks both content and sitemap URLs correctly' do
      seen_urls.add?(url1, type: :content)
      seen_urls.add?(url2, type: :content)
      seen_urls.add?(url3, type: :sitemap)
      
      expect(seen_urls.content_count).to eq(2)
      expect(seen_urls.count).to eq(3)
    end
  end

  describe '#count' do
    it 'returns 0 for empty set' do
      expect(seen_urls.count).to eq(0)
    end

    it 'returns correct count after adding URLs' do
      seen_urls.add?(url1)
      seen_urls.add?(url2)
      expect(seen_urls.count).to eq(2)
    end
  end

  describe '#content_count' do
    it 'returns 0 for empty set' do
      expect(seen_urls.content_count).to eq(0)
    end

    it 'returns correct count after adding content URLs' do
      seen_urls.add?(url1, type: :content)
      seen_urls.add?(url2, type: :content)
      expect(seen_urls.content_count).to eq(2)
    end

    it 'does not count non-content URLs' do
      seen_urls.add?(url1, type: :content)
      seen_urls.add?(url2, type: :sitemap)
      seen_urls.add?(url3, type: :robots_txt)
      expect(seen_urls.content_count).to eq(1)
    end
  end

  describe '#clear' do
    it 'clears both seen_urls and content_urls' do
      seen_urls.add?(url1, type: :content)
      seen_urls.add?(url2, type: :sitemap)
      
      seen_urls.clear
      
      expect(seen_urls.count).to eq(0)
      expect(seen_urls.content_count).to eq(0)
    end
  end

  describe '#delete' do
    it 'removes URL from seen_urls' do
      seen_urls.add?(url1)
      seen_urls.delete(url1)
      expect(seen_urls.count).to eq(0)
    end
  end
end
