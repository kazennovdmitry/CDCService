using RepoDb;
using RepoDb.Attributes;

namespace CDC.CDCService.Models;

public class DocumentOperationResult
{
    [Map("__$start_lsn")]
    public byte[] StartLsn { get; set; } = Array.Empty<byte>();

    [Map("__$seqval")]
    public byte[] SeqVal { get; set; } = Array.Empty<byte>();

    [Map("Id")]
    public int DocumentId { get; set; }
    
    [Map("__$operation")]
    public int OperationId { get; set; }
    
    [Map("DocPathFolderId")]
    public int DocPathFolderId { get; set; }
    
    [Map("DocClass")]
    public int DocClassId { get; set; }
}