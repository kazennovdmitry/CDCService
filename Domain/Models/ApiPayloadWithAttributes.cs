namespace CDC.CoreLogic.Models;

public class ApiPayloadWithAttributes
{
    public string? Payload { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();
}