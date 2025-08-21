require 'net/http'
require 'uri'
require 'json'

class S3TestCaseService
  class << self
    def download_and_parse_test_cases(s3_urls)
      return [] if s3_urls.blank?
      
      urls = parse_s3_urls(s3_urls)
      test_cases = []
      
      urls.each do |url|
        begin
          content = download_file(url)
          parsed_cases = parse_test_case_content(content, url)
          test_cases.concat(parsed_cases)
        rescue => e
          Rails.logger.error "Failed to download test case from #{url}: #{e.message}"
          raise "Failed to download test case from #{url}: #{e.message}"
        end
      end
      
      test_cases
    end

    private

    def parse_s3_urls(s3_urls)
      # Handle different input formats:
      # - Single URL string
      # - JSON array of URLs
      # - Newline separated URLs
      
      return [] if s3_urls.blank?
      
      urls = []
      
      # Try parsing as JSON first
      begin
        parsed = JSON.parse(s3_urls)
        if parsed.is_a?(Array)
          urls = parsed
        elsif parsed.is_a?(String)
          urls = [parsed]
        end
      rescue JSON::ParserError
        # Not JSON, try other formats
        if s3_urls.include?("\n")
          # Newline separated
          urls = s3_urls.split("\n").map(&:strip).reject(&:empty?)
        else
          # Single URL
          urls = [s3_urls.strip]
        end
      end
      
      # Validate URLs
      urls.select { |url| valid_s3_url?(url) }
    end
    
    def valid_s3_url?(url)
      return false if url.blank?
      
      # Basic S3 URL validation
      s3_patterns = [
        /\Ahttps:\/\/.*\.s3\..*\.amazonaws\.com\//,
        /\Ahttps:\/\/s3\..*\.amazonaws\.com\//,
        /\Ahttps:\/\/.*\.s3-.*\.amazonaws\.com\//
      ]
      
      s3_patterns.any? { |pattern| url.match?(pattern) }
    end
    
    def download_file(url)
      uri = URI(url)
      
      Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
        request = Net::HTTP::Get.new(uri)
        request['User-Agent'] = 'Judge0-TestCase-Downloader/1.0'
        
        response = http.request(request)
        
        unless response.code == '200'
          raise "HTTP #{response.code}: #{response.message}"
        end
        
        response.body
      end
    end
    
    def parse_test_case_content(content, source_url)
      # Support multiple formats:
      # 1. JSON format: {"test_cases": [{"input": "...", "output": "..."}, ...]}
      # 2. Plain text format: input and output separated by "---" or similar
      # 3. ZIP format: multiple files (future enhancement)
      
      test_cases = []
      
      begin
        # Try JSON format first
        data = JSON.parse(content)
        
        if data.is_a?(Hash) && data['test_cases']
          data['test_cases'].each_with_index do |test_case, index|
            test_cases << {
              input: test_case['input'] || test_case['stdin'] || '',
              output: test_case['output'] || test_case['expected_output'] || '',
              name: test_case['name'] || "Test Case #{index + 1}",
              source_url: source_url
            }
          end
        elsif data.is_a?(Array)
          # Array of test cases
          data.each_with_index do |test_case, index|
            test_cases << {
              input: test_case['input'] || test_case['stdin'] || '',
              output: test_case['output'] || test_case['expected_output'] || '',
              name: test_case['name'] || "Test Case #{index + 1}",
              source_url: source_url
            }
          end
        end
      rescue JSON::ParserError
        # Try plain text format
        test_cases = parse_plain_text_format(content, source_url)
      end
      
      test_cases
    end
    
    def parse_plain_text_format(content, source_url)
      # Simple format:
      # INPUT:
      # <input data>
      # OUTPUT:
      # <expected output>
      # ---
      # INPUT:
      # <next input>
      # ...
      
      test_cases = []
      sections = content.split(/^---+$/m)
      
      sections.each_with_index do |section, index|
        lines = section.strip.split("\n")
        input_lines = []
        output_lines = []
        current_section = nil
        
        lines.each do |line|
          if line.strip.upcase.start_with?('INPUT:')
            current_section = :input
            # Include the content after "INPUT:" if any
            content_after = line.sub(/^INPUT:\s*/i, '').strip
            input_lines << content_after unless content_after.empty?
          elsif line.strip.upcase.start_with?('OUTPUT:')
            current_section = :output
            # Include the content after "OUTPUT:" if any
            content_after = line.sub(/^OUTPUT:\s*/i, '').strip
            output_lines << content_after unless content_after.empty?
          else
            case current_section
            when :input
              input_lines << line
            when :output
              output_lines << line
            end
          end
        end
        
        next if input_lines.empty? && output_lines.empty?
        
        test_cases << {
          input: input_lines.join("\n"),
          output: output_lines.join("\n"),
          name: "Test Case #{index + 1}",
          source_url: source_url
        }
      end
      
      # If no structured format found, treat entire content as single test case
      if test_cases.empty? && content.strip.present?
        test_cases << {
          input: content.strip,
          output: '',
          name: 'Test Case 1',
          source_url: source_url
        }
      end
      
      test_cases
    end
  end
end