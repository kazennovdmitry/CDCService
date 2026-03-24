namespace CDC.CDCService.Models;

public class LsnWithTableName
{
    public byte[]? Lsn { get; set; }
    public string? TableName { get; set; }
    
    public string Hexadecimal => GetHexadecimal();

    private string GetHexadecimal()
    {
        return Lsn is null? "0x000000000000000000" : $"0x{BitConverter.ToString(Lsn).Replace("-", "")}";
    }
}