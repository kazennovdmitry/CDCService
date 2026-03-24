using CDC.CoreLogic.Models;

namespace CDC.CoreLogic.Interfaces;


public interface IRepositoryManager
{
    /// <summary>
    /// Checks for changes in API document status after Documents.DocPathFolderId update.
    /// Provides the old status value and the new status value.
    /// Returns null for statuses that have no mapping in api.v_ApiTransferStatus.
    /// </summary>
    public Task<(string? OldApiStatus, string? NewApiStatus)> CheckIfExternalStatusChanged(int oldDocPathFolderId, int newDocPathFolderId);
    
    /// <summary>
    /// Returns OpenAPI TransferInternal payload in JSON along with PubSub message attributes.
    /// </summary>
    public Task<List<ApiPayloadWithAttributes>> GetTransferInternal(DocumentOperation updateNewValues);

    public Task<List<ApiPayloadWithAttributes>> GetTransfersOutgoing(DocumentOperation updateNewValues);

    public Task<List<ApiPayloadWithAttributes>> GetTransfersIncoming(DocumentOperation updateNewValues);
}