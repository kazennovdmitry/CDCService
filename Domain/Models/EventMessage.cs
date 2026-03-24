using System.Text.Json.Serialization;

namespace CDC.CoreLogic.Models;

public class EventMessage
{
    [JsonPropertyName("data")]
    public string Data { get; set; } = string.Empty;

    [JsonPropertyName("attributes")] 
    public Dictionary<string, string> Attributes { get; set; }
}