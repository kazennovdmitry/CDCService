using Confluent.Kafka;
using CDC.CoreLogic.Interfaces;
using CDC.CoreLogic.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using CDC.Kafka.Models;

/*
 * Kafka integration Example
 *
 * This demonstrates a generic Kafka producer pattern with configurable authentication.
 *
 */

namespace CDC.Kafka;

public class KafkaPublisherService : IPublisherService, IDisposable
{
    private readonly ILogger<KafkaPublisherService> _logger;
    private readonly KafkaConfig _config;
    private readonly IProducer<string, string> _producer;
    private bool _disposed;

    // Cache service account email (doesn't change during runtime)
    private string? _cachedServiceAccountEmail;
    private readonly object _emailCacheLock = new object();

    public KafkaPublisherService(
        IConfiguration configuration,
        ILogger<KafkaPublisherService> logger)
    {
        _logger = logger;
        _config = configuration.GetSection("Kafka").Get<KafkaConfig>()
            ?? throw new InvalidOperationException("Kafka configuration is missing");

        _producer = CreateProducer();
        _logger.LogInformation(
            "Kafka producer initialized. Topic: {Topic}, Bootstrap: {Bootstrap}, Auth: {AuthMethod}",
            _config.Topic,
            _config.BootstrapServers,
            _config.AuthenticationMethod);

        // Validate connection and topic in background
        ValidateConnectionAsync().ConfigureAwait(false);
    }

    private async Task ValidateConnectionAsync()
    {
        await Task.Yield(); // Allow constructor to complete first
        try
        {
            _logger.LogInformation("Validating Kafka connection...");

            var adminConfig = CreateAdminConfig();
            using var adminClient = CreateAdminClient(adminConfig);
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            _logger.LogInformation("Kafka connected successfully! Broker: {Broker}, OriginatingBrokerId: {BrokerId}",
                metadata.Brokers.FirstOrDefault()?.Host ?? "unknown",
                metadata.OriginatingBrokerId);

            // Check if topic exists
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _config.Topic);
            if (topicMetadata != null)
            {
                _logger.LogInformation("Topic '{Topic}' exists with {PartitionCount} partition(s)",
                    _config.Topic,
                    topicMetadata.Partitions.Count);
            }
            else
            {
                _logger.LogWarning("Topic '{Topic}' does not exist! Available topics: {Topics}",
                    _config.Topic,
                    string.Join(", ", metadata.Topics.Select(t => t.Topic)));
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to validate Kafka connection");
        }
    }

    private AdminClientConfig CreateAdminConfig()
    {
        return _config.AuthenticationMethod switch
        {
            AuthenticationMethod.OAuth => new AdminClientConfig
            {
                BootstrapServers = _config.BootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
            },
            AuthenticationMethod.SaslUsernamePassword => new AdminClientConfig
            {
                BootstrapServers = _config.BootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = _config.SaslUsername,
                SaslPassword = _config.SaslPassword,
            },
            _ => new AdminClientConfig
            {
                BootstrapServers = _config.BootstrapServers,
                SecurityProtocol = SecurityProtocol.Ssl
            }
        };
    }

    private IAdminClient CreateAdminClient(AdminClientConfig config)
    {
        if (_config.AuthenticationMethod == AuthenticationMethod.OAuth)
        {
            return new AdminClientBuilder(config)
                .SetOAuthBearerTokenRefreshHandler(OAuthBearerTokenRefreshHandler)
                .Build();
        }

        return new AdminClientBuilder(config).Build();
    }

    private IProducer<string, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _config.BootstrapServers,

            // Producer reliability settings
            EnableIdempotence = _config.EnableIdempotence,
            Acks = (Acks)_config.Acks,
            MessageTimeoutMs = _config.MessageTimeoutMs,
            MessageSendMaxRetries = _config.Retries,
            RetryBackoffMs = _config.RetryBackoffMs,
        };

        switch (_config.AuthenticationMethod)
        {
            case AuthenticationMethod.OAuth:
                config.SecurityProtocol = SecurityProtocol.SaslSsl;
                config.SaslMechanism = SaslMechanism.OAuthBearer;
                break;
            case AuthenticationMethod.SaslUsernamePassword:
                config.SecurityProtocol = SecurityProtocol.SaslSsl;
                config.SaslMechanism = SaslMechanism.Plain;
                config.SaslUsername = _config.SaslUsername!;
                config.SaslPassword = _config.SaslPassword!;
                break;
            default:
                config.SecurityProtocol = SecurityProtocol.Ssl;
                break;
        }

        // Connection stability settings
        config.SocketKeepaliveEnable = true;
        config.ConnectionsMaxIdleMs = 540000;
        config.MetadataMaxAgeMs = 180000;
        config.ReconnectBackoffMs = 100;
        config.ReconnectBackoffMaxMs = 10000;

        var builder = new ProducerBuilder<string, string>(config);

        if (_config.AuthenticationMethod == AuthenticationMethod.OAuth)
        {
            builder.SetOAuthBearerTokenRefreshHandler(OAuthBearerTokenRefreshHandler);
        }

        return builder
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError("Kafka producer error: {Code} - {Reason}",
                    error.Code, error.Reason);
            })
            .SetLogHandler((_, logMessage) =>
            {
                var level = logMessage.Level switch
                {
                    SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical
                        => LogLevel.Critical,
                    SyslogLevel.Error => LogLevel.Error,
                    SyslogLevel.Warning or SyslogLevel.Notice => LogLevel.Warning,
                    SyslogLevel.Info => LogLevel.Information,
                    _ => LogLevel.Debug
                };
                _logger.Log(level, "Kafka: [{Facility}] {Message}",
                    logMessage.Facility, logMessage.Message);
            })
            .Build();
    }

    /// <summary>
    /// OAuth bearer token refresh handler for Kafka authentication.
    /// Builds a JWT token in the format: header.payload.access_token
    /// </summary>
    private void OAuthBearerTokenRefreshHandler(IClient client, string oauthBearerConfig)
    {
        try
        {
            _logger.LogDebug("OAuth token refresh initiated");

            // Get service account email for the principal
            var serviceAccountEmail = GetServiceAccountEmail();
            _logger.LogInformation("OAuth token refresh - using principal: {Principal}", serviceAccountEmail);

            // Calculate token expiry (tokens typically expire in 1 hour)
            var expiryMs = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds();

            // Build JWT token in standard format: header.payload.access_token
            var header = new { typ = "JWT", alg = "OAUTHBEARER" };
            var payload = new
            {
                exp = expiryMs / 1000,
                iss = "KafkaClient",
                iat = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                sub = serviceAccountEmail
            };

            var headerBase64 = Base64UrlEncode(JsonSerializer.Serialize(header));
            var payloadBase64 = Base64UrlEncode(JsonSerializer.Serialize(payload));
            var tokenBase64 = Base64UrlEncode("access_token_placeholder");

            var kafkaToken = $"{headerBase64}.{payloadBase64}.{tokenBase64}";

            // Set the token on the Kafka client
            client.OAuthBearerSetToken(
                tokenValue: kafkaToken,
                lifetimeMs: expiryMs,
                principalName: serviceAccountEmail);

            _logger.LogDebug("OAuth token refreshed for principal: {Principal}", serviceAccountEmail);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to refresh OAuth bearer token");
            client.OAuthBearerSetTokenFailure($"Token refresh failed: {ex.Message}");
        }
    }

    private string GetServiceAccountEmail()
    {
        // Return cached value if available (email doesn't change during runtime)
        if (!string.IsNullOrEmpty(_cachedServiceAccountEmail))
            return _cachedServiceAccountEmail;

        lock (_emailCacheLock)
        {
            // Double-check after acquiring lock
            if (!string.IsNullOrEmpty(_cachedServiceAccountEmail))
                return _cachedServiceAccountEmail;

            // Check if configured in appsettings
            if (!string.IsNullOrEmpty(_config.ServiceAccountEmail))
            {
                _cachedServiceAccountEmail = _config.ServiceAccountEmail;
                _logger.LogInformation("Service account email configured.");
                return _cachedServiceAccountEmail;
            }

            // Fallback: generate a generic identifier
            _cachedServiceAccountEmail = $"kafka-client-{Environment.MachineName ?? "unknown"}";
            _logger.LogWarning("No service account email configured, using fallback: {Fallback}", _cachedServiceAccountEmail);
            return _cachedServiceAccountEmail;
        }
    }

    private static string Base64UrlEncode(string input)
    {
        var bytes = Encoding.UTF8.GetBytes(input);
        return Convert.ToBase64String(bytes)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
    }

   public async Task<string> PublishMessageWithCustomAttributesAsync(EventMessage message)
    {
        try
        {
            // Create Kafka message with headers from attributes
            var kafkaMessage = new Message<string, string>
            {
                // Use event type as partition key for ordering
                Key = message.Attributes.TryGetValue("eventType", out var eventType)
                    ? eventType
                    : Guid.NewGuid().ToString(),
                Value = message.Data, // Base64-encoded JSON payload
                Headers = ConvertAttributesToHeaders(message.Attributes)
            };

            // Produce message to Kafka
            var deliveryResult = await _producer.ProduceAsync(_config.Topic, kafkaMessage);

            var messageId = $"{deliveryResult.Partition.Value}-{deliveryResult.Offset.Value}";

            _logger.LogDebug(
                "Message published to Kafka. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}",
                deliveryResult.Topic,
                deliveryResult.Partition.Value,
                deliveryResult.Offset.Value,
                kafkaMessage.Key);

            return messageId;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Failed to publish message to Kafka. Topic: {Topic}, Error: {Code} - {Reason}",
                _config.Topic,
                ex.Error.Code,
                ex.Error.Reason);
            throw;
        }
    }

    private static Headers ConvertAttributesToHeaders(Dictionary<string, string> attributes)
    {
        var headers = new Headers();
        foreach (var kvp in attributes)
        {
            headers.Add(kvp.Key, Encoding.UTF8.GetBytes(kvp.Value));
        }
        return headers;
    }

    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            // Flush any pending messages before disposing
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during Kafka producer disposal");
        }

        _disposed = true;
        _logger.LogInformation("Kafka producer disposed");
    }
}
