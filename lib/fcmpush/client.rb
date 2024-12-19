require 'fcmpush/exceptions'
require 'fcmpush/batch'
require 'fcmpush/json_response'
require 'fcmpush/batch_response'

module Fcmpush
  V1_ENDPOINT_PREFIX = '/v1/projects/'.freeze
  V1_ENDPOINT_SUFFIX = '/messages:send'.freeze
  TOPIC_DOMAIN = 'https://iid.googleapis.com'.freeze
  TOPIC_ENDPOINT_PREFIX = '/iid/v1'.freeze
  BATCH_ENDPOINT = '/batch'.freeze

  class Client
    include Batch
    attr_reader :domain, :path, :connection, :configuration, :server_key, :access_token, :access_token_expiry

    def initialize(domain, project_id, configuration, **options)
      @domain = domain
      @project_id = project_id
      @path = V1_ENDPOINT_PREFIX + project_id.to_s + V1_ENDPOINT_SUFFIX
      @options = {}.merge(options)
      @configuration = configuration.dup
      access_token_response = v1_authorize
      @access_token = access_token_response['access_token']
      @access_token_expiry = Time.now.utc + access_token_response['expires_in']
      @server_key = configuration.server_key
      @connection = Net::HTTP::Persistent.new
    end

    def v1_authorize
      @auth ||= if configuration.json_key_io
                  io = if configuration.json_key_io.respond_to?(:read)
                         configuration.json_key_io
                       else
                         File.open(configuration.json_key_io)
                       end
                  Google::Auth::ServiceAccountCredentials.make_creds(
                    json_key_io: io,
                    scope: configuration.scope
                  )
                else
                  # from ENV
                  Google::Auth::ServiceAccountCredentials.make_creds(scope: configuration.scope)
                end
      @auth.fetch_access_token
    end

    def push(body, query: {}, headers: {})
      uri, request = make_push_request(body, query, headers)
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def subscribe(topic, *instance_ids, query: {}, headers: {})
      uri, request = make_subscription_request(topic, *instance_ids, :subscribe, query, headers)
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def subscribe_v1(topic, *instance_ids, query: {}, headers: {})
      uri, request = make_subscription_request(topic, *instance_ids, :subscribe, query, headers, 'v1')
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def unsubscribe(topic, *instance_ids, query: {}, headers: {})
      uri, request = make_subscription_request(topic, *instance_ids, :unsubscribe, query, headers)
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def unsubscribe_v1(topic, *instance_ids, query: {}, headers: {})
      uri, request = make_subscription_request(topic, *instance_ids, :unsubscribe, query, headers, 'v1')
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def create_group_v1(args: {}, query: {}, headers: {})
      args = args.with_indifferent_access
      manage_device_group(args.merge('operation' => 'create'), query, headers)
    end

    def add_group_v1(args: {}, query: {}, headers: {})
      args = args.with_indifferent_access
      manage_device_group(args.merge('operation' => 'add'), query, headers)
    end

    def remove_group_v1(args: {}, query: {}, headers: {})
      args = args.with_indifferent_access
      manage_device_group(args.merge('operation' => 'remove'), query, headers)
    end

    def batch_push(messages, query: {}, headers: {})
      uri, request = make_batch_request(messages, query, headers)
      response = exception_handler(connection.request(uri, request))
      BatchResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    def get_info(instance_id, headers: {})
      uri, request = make_info_request(instance_id, headers)
      response = exception_handler(connection.request(uri, request))
      JsonResponse.new(response)
    rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
      raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
    end

    private

      def make_push_request(body, query, headers)
        uri = URI.join(domain, path)
        uri.query = URI.encode_www_form(query) unless query.empty?

        access_token_refresh
        headers = v1_authorized_header(headers)
        post = Net::HTTP::Post.new(uri, headers)
        post.body = body.is_a?(String) ? body : body.to_json

        [uri, post]
      end

      def make_subscription_request(topic, instance_ids, type, query, headers, api_version=nil)
        suffix = type == :subscribe ? ':batchAdd' : ':batchRemove'

        uri = URI.join(TOPIC_DOMAIN, TOPIC_ENDPOINT_PREFIX + suffix)
        uri.query = URI.encode_www_form(query) unless query.empty?

        headers = api_version.eql?('v1') ? v1_authorized_header(headers) : legacy_authorized_header(headers)
        post = Net::HTTP::Post.new(uri, headers)
        post.body = make_subscription_body(topic, *instance_ids)

        [uri, post]
      end

      def access_token_refresh
        return if access_token_expiry > Time.now.utc + 300

        access_token_response = v1_authorize
        @access_token = access_token_response['access_token']
        @access_token_expiry = Time.now.utc + access_token_response['expires_in']
      end

      def v1_authorized_header(headers)
        headers.merge('Content-Type' => 'application/json',
                      'Accept' => 'application/json',
                      'access_token_auth' => 'true',
                      'Authorization' => "Bearer #{access_token}")
      end

      def legacy_authorized_header(headers)
        headers.merge('Content-Type' => 'application/json',
                      'Accept' => 'application/json',
                      'Authorization' => "Bearer key=#{server_key}")
      end

      def exception_handler(response)
        error = STATUS_TO_EXCEPTION_MAPPING[response.code]
        raise error.new("Receieved an error response #{response.code} #{error.to_s.split('::').last}: #{response.body}", response) if error

        response
      end

      def make_subscription_body(topic, *instance_ids)
        topic = topic.match(%r{^/topics/}) ? topic : '/topics/' + topic
        {
          to: topic,
          registration_tokens: instance_ids
        }.to_json
      end

      def make_batch_request(messages, query, headers)
        uri = URI.join(domain, BATCH_ENDPOINT)
        uri.query = URI.encode_www_form(query) unless query.empty?

        access_token_refresh
        headers = v1_authorized_header(headers)
        post = Net::HTTP::Post.new(uri, headers)
        post['Content-Type'] = "multipart/mixed; boundary=#{::Fcmpush::Batch::PART_BOUNDRY}"
        post.body = make_batch_payload(messages, headers)

        [uri, post]
      end

      def make_info_request(instance_id, headers)
        headers = v1_authorized_header(headers)
        uri = URI.join(TOPIC_DOMAIN, "/iid/info/#{instance_id}")
        get = Net::HTTP::Get.new(uri, headers)

        [uri, get]
      end

      def make_subscription_group_request(args, query, headers)
        uri = 'https://fcm.googleapis.com/fcm/notification'
        uri.query = URI.encode_www_form(query) unless query.empty?
      
        headers = v1_authorized_header(headers)
        post = Net::HTTP::Post.new(uri, headers)
        post.body = make_subscription_group_body(args)
      
        [uri, post]
      end
      
      def make_subscription_group_body(options)
        params = {
          operation: options['operation'], #[create, add, remove]
          notification_key_name: options['notification_key_name'],
          registration_ids: options['registration_ids'] # devices user
        }
      
        if ['add', 'remove'].include?(options['operation'])
          params = params.merge('notification_key' => options['notification_key'])
        end
      
        params.to_json
      end
      
      def manage_device_group(args, query, headers)
        uri, request = make_subscription_group_request(args, query, headers)
        response = exception_handler(connection.request(uri, request))
        JsonResponse.new(response)
      rescue Timeout::Error, Errno::EINVAL, Errno::ECONNRESET, EOFError, Net::HTTPBadResponse, Net::HTTPHeaderSyntaxError, Net::ProtocolError => e
        raise NetworkError, "A network error occurred: #{e.class} (#{e.message})"
      end
  end
end
