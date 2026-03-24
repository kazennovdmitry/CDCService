using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace CDC.Repository.Services;

public class SqlQueryLoader
{
    private readonly Dictionary<string, string> _queries;

    public SqlQueryLoader(string filePath)
    {
        var yaml = File.ReadAllText(filePath);
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();

        var data = deserializer.Deserialize<Dictionary<string, Dictionary<string, string>>>(yaml);
        _queries = data["queries"];
    }

    public string GetQuery(string key)
    {
        return _queries.TryGetValue(key, out var query) ? query : throw new KeyNotFoundException($"Query '{key}' not found.");
    }
}