using System;
using System.Text.Json;
using CDC.CoreLogic.Interfaces;
using CDC.CoreLogic.Models;
using CDC.Repository.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

using RepoDb;

namespace CDC.Repository;

public class RepositoryManager : IRepositoryManager
{
    private readonly ILogger<RepositoryManager> _logger;
    private readonly SqlQueryLoader _queryLoader;
    private readonly string? _connectionString;
    
    public RepositoryManager(IConfiguration configuration, 
        ILogger<RepositoryManager> logger, 
        IServiceProvider provider)
    {
        _logger = logger;
        _queryLoader = GetQueryLoader();
        _connectionString = configuration.GetConnectionString("Default");
    }

    private SqlQueryLoader GetQueryLoader()
    {
        return new SqlQueryLoader(Path.Combine(AppContext.BaseDirectory, "settings/sql", "repositoryQueries.yaml"));
    }
    
    /// <summary>
    /// Checks if the API Document status changed after the internal Document status change.
    /// Returns null for statuses that have no mapping in api.v_ApiTransferStatus.
    /// </summary>
    public async Task<(string? OldApiStatus, string? NewApiStatus)> CheckIfExternalStatusChanged(int oldDocPathFolderId, int newDocPathFolderId)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.EnsureOpenAsync();

        var query = _queryLoader.GetQuery("GetExternalStatuses");

        var parameters = new
        {
            oldValue = oldDocPathFolderId,
            newValue = newDocPathFolderId
        };

        var result = await connection.ExecuteQueryAsync<ExternalStatus>(query, parameters);
        var status = result.FirstOrDefault();

        return (status?.OldValue, status?.NewValue);
    }

    /// <summary>
    /// Returns GetTransferInternal API payload by Document ID without the validation.
    /// </summary>
    public async Task<List<ApiPayloadWithAttributes>> GetTransferInternal(DocumentOperation updateNewValues)
    {
        var list = new List<ApiPayloadWithAttributes>();

        try
        {
            var transferDto = await GetInternalTransferByDocumentId(updateNewValues.DocumentId);
            if (transferDto is not null)
            {
                var senderId = transferDto.DebitClientId;
                var recipientId = transferDto.CreditClientId;
            
                if (senderId > 0 && recipientId > 0 && 
                    (senderId != recipientId || recipientId == senderId))
                {
                    AddPayload(list, JsonSerializer.Serialize(transferDto), 
                        transferDto.DebitClientId,
                        transferDto.DebitAccountId,
                        "transfer.outgoing.changed"
                    );
                
                    AddPayload(list,  JsonSerializer.Serialize(transferDto), 
                        transferDto.CreditClientId,
                        transferDto.CreditAccountId,
                        "transfer.incoming.created");
                }
                else if (senderId > 0 && recipientId == 0)
                {
                    AddPayload(list, JsonSerializer.Serialize(transferDto), 
                        transferDto.DebitClientId, 
                        transferDto.DebitAccountId,
                        "transfer.outgoing.changed");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,$"Error getting the GetTransferInternal API payload for document {updateNewValues.DocumentId}.");
        }

        return list;
    }

    /// <summary>
    /// Returns GetTransfersInternational API payload by Document ID without the validation.
    /// </summary>
    public async Task<List<ApiPayloadWithAttributes>> GetTransfersOutgoing(DocumentOperation updateNewValues)
    {
        var list = new List<ApiPayloadWithAttributes>();

        try
        {
            var transferDto = await GetInternationalTransferByDocumentId(updateNewValues.DocumentId);
            if (transferDto is not null)
            {
                AddPayload(list, JsonSerializer.Serialize(transferDto), 
                    transferDto.CreditClientId,
                    transferDto.CreditAccountId,
                    "transfer.outgoing.changed");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,$"Error getting the GetTransferInternational API payload for document {updateNewValues.DocumentId}.");
        }
        
        return list;
    }
    
    /// <summary>
    /// Returns GetTransfersIncoming API payload by Document ID without the validation.
    /// </summary>
    public async Task<List<ApiPayloadWithAttributes>> GetTransfersIncoming(DocumentOperation updateNewValues)
    {
        var list = new List<ApiPayloadWithAttributes>();

        try
        {
            var transferDto = await GetIncomingTransferByDocumentId(updateNewValues.DocumentId);
            if (transferDto is not null)
            {
                AddPayload(list, JsonSerializer.Serialize(transferDto),
                    transferDto.DebitClientId,
                    transferDto.DebitAccountId,
                    "transfer.incoming.changed");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,$"Error getting the Incoming Transfer API payload for document {updateNewValues.DocumentId}.");
        }

        return list;
    }

    private async Task<Transfer> GetInternationalTransferByDocumentId(int documentId)
    {
        // there should be db call
        return new Transfer(documentId);
    }
    
    private async Task<Transfer?> GetInternalTransferByDocumentId(int documentId)
    {
        // there should be db call
        return new Transfer(documentId);
    }
    
    private async Task<Transfer> GetUkTransferByDocumentId(int documentId)
    {
        // there should be db call
        return new Transfer(documentId);
    }
    
    private async Task<Transfer> GetIncomingTransferByDocumentId(int documentId)
    {
        // there should be db call
        return new Transfer(documentId);
    }

    private void AddPayload(List<ApiPayloadWithAttributes> list, 
        string jsonTransfer, 
        int clientId, int accountId, string type)
    {
        list.Add(new ApiPayloadWithAttributes()
        {
            Payload = jsonTransfer,
            Attributes = new Dictionary<string, string>
            {
                ["clientId"] = clientId.ToString(),
                ["accountId"] = accountId.ToString(),
                ["type"] = type
            }
        });
    }
}