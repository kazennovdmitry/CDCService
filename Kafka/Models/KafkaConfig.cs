namespace CDC.Kafka.Models;

public enum AuthenticationMethod
{
    None,
    SaslUsernamePassword,
    OAuth
}

public class KafkaConfig
{
    /// <summary>
    /// Kafka bootstrap servers
    /// Format: "host:port"
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Kafka topic for CDC events
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Authentication method: None (SSL), SASL username/password, or OAuth
    /// </summary>
    public AuthenticationMethod AuthenticationMethod { get; set; } = AuthenticationMethod.None;

    /// <summary>
    /// Enable idempotent producer (exactly-once semantics)
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Acknowledgment level: 0 (none), 1 (leader), -1 (all)
    /// </summary>
    public int Acks { get; set; } = -1;

    /// <summary>
    /// Message timeout in milliseconds
    /// </summary>
    public int MessageTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Number of retries for failed messages
    /// </summary>
    public int Retries { get; set; } = 3;

    /// <summary>
    /// Retry backoff in milliseconds
    /// </summary>
    public int RetryBackoffMs { get; set; } = 100;

    /// <summary>
    /// SASL username for authentication (required when AuthenticationMethod is SaslUsernamePassword)
    /// </summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    /// SASL password for authentication (required when AuthenticationMethod is SaslUsernamePassword)
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>
    /// Service account email or principal identifier for OAuth authentication (optional)
    /// If not set, will be auto-generated
    /// </summary>
    public string? ServiceAccountEmail { get; set; }
}
