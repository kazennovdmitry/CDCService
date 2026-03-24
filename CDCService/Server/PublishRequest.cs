namespace CDC.CDCService.Server;

public class PublishRequest
{
    public int DocumentId { get; set; }
    public List<RequestDocumentOperation> Operations { get; set; }
}

public class RequestDocumentOperation
{
    public int OperationId { get; set; }
    public int DocPathFolderId { get; set; }
    public int DocClassId { get; set; }
}