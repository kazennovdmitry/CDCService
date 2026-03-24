namespace CDC.CoreLogic.Models;

public class DocumentOperation
{
    public int DocumentId { get; set; }
    public int OperationId { get; set; }
    public int Status { get; set; }
    public int DocClassId { get; set; }
    public string? DocClassName { get; set; }
}