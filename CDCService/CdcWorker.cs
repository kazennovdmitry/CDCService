using CDC.CoreLogic.Interfaces;
using Microsoft.Data.SqlClient;
using CDC.Repository.Services;
using CDC.CDCService.Models;
using CDC.CoreLogic.Models;

using RepoDb;

namespace CDC.CDCService;

/*
 * CDC Worker Example
 *
 * This is a simplified demonstration of SQL Server CDC processing
 * using LSN (Log Sequence Number) tracking. This code is provided
 * as an educational example and does not contain any proprietary logic.
 *
 */

public class CdcWorker : BackgroundService
{
    private readonly ILogger<CdcWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly SqlQueryLoader _queryLoader;
    private readonly IMainHelper _helper;
    private readonly string? _connectionString;
    
    private Dictionary<int, string> _catalogedDocumentTypes = new();
    private LsnWithTableName _currentLsnContext = new();
    // Configuration settings
    private readonly int _backoffIntervalMs;
    private readonly int _transientFaultRetryMs;
    private readonly int _startupRetryWindowMs;
    private readonly int _fetchWindowSize;
    
    public CdcWorker(IConfiguration configuration, ILogger<CdcWorker> logger, IMainHelper helper)
    {
        _logger = logger;
        _configuration = configuration;
        _connectionString = configuration.GetConnectionString("Default");
        _queryLoader = new SqlQueryLoader(Path.Combine(AppContext.BaseDirectory, "settings/sql", "CdcQueries.yaml"));
        _helper = helper;
        var cdcConfig = configuration.GetSection("CdcService");
        _backoffIntervalMs = cdcConfig.GetValue("ProcessingDelaySeconds", 1);
        _transientFaultRetryMs = cdcConfig.GetValue("ErrorRetryDelaySeconds", 5);
        _startupRetryWindowMs = cdcConfig.GetValue("InitialRetryDelaySeconds", 10);
        _fetchWindowSize = cdcConfig.GetValue("BatchMaxLsns", 100);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitializeAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            int delay = _backoffIntervalMs;
            try
            {
                await ProcessDocumentChangesAsync(stoppingToken);
            }
            catch (SqlException ex)
            {
                _logger.LogError(ex, "SQL error while tracking changes.");
                delay = _transientFaultRetryMs;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while tracking changes.");
                delay = _transientFaultRetryMs;
            }
            await Task.Delay(TimeSpan.FromSeconds(delay), stoppingToken);
        }
    }
    
    private async Task InitializeAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(stoppingToken);
                await ClearProcessedLsnTraceAsync(connection, stoppingToken);
                LoadSupportedDocumentClasses();

                _logger.LogInformation("Initial preparations were successful.");
                return;
            }
            catch (SqlException ex)
            {
                _logger.LogError(ex, "SQL error during initial preparations.");
                await Task.Delay(TimeSpan.FromSeconds(_startupRetryWindowMs), stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error during initial preparations.");
                await Task.Delay(TimeSpan.FromSeconds(_startupRetryWindowMs), stoppingToken);
            }
        }
    }
    
    private void LoadSupportedDocumentClasses()
    {
        var paymentDocClassNames = _configuration.GetSection("PaymentDocClasses").Get<List<string>>() ?? new List<string>();
        
        // This is for demo purposes only. In real scenario there should be a request to actual IDs of document classes.
        _helper.PaymentDocClasses = paymentDocClassNames.ToDictionary(
            name => paymentDocClassNames.IndexOf(name), 
            pair => pair);
        
        _catalogedDocumentTypes = _helper.PaymentDocClasses;
    }
    
    private async Task ClearProcessedLsnTraceAsync(SqlConnection connection, CancellationToken stoppingToken)
    {
        var query = _queryLoader.GetQuery("ClearProcessedLSNs");
        await connection.ExecuteNonQueryAsync(query, cancellationToken: stoppingToken);
    }
    
    private async Task ProcessDocumentChangesAsync(CancellationToken stoppingToken)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(stoppingToken);

        // Determine from and to LSN for this batch
        var lastProcessedLsn = await GetLastProcessedLsnAsync(connection, stoppingToken);
        var fromLsn = lastProcessedLsn.Lsn ?? new byte[10]; // 0x000000000000000000
        var supportedDocClassIdsCsv = string.Join(',', _catalogedDocumentTypes.Keys);

        var toLsnWithTable = await GetNextBatchLsnAsync(connection, stoppingToken, fromLsn, _fetchWindowSize, supportedDocClassIdsCsv);
        if (toLsnWithTable.Lsn is null)
        {
            return;
        }

        // Fetch ordered operations for the whole range in one call, filtered by supported doc classes
        var allOperations = await FetchPendingChangesAsync(connection, stoppingToken, fromLsn, toLsnWithTable.Lsn, supportedDocClassIdsCsv);

        // Ensure deterministic order: by StartLsn then by SeqVal within the same LSN
        allOperations = allOperations
            .OrderBy(op => op.StartLsn, ByteArrayLexComparer.Instance)
            .ThenBy(op => op.SeqVal, ByteArrayLexComparer.Instance)
            .ToList();

        if (allOperations.Count == 0)
        {
            // No supported document classes in this batch - advance watermark to skip these changes
            _logger.LogDebug("No supported document classes found in batch. Advancing LSN from {FromLsn} to {ToLsn}",
                Convert.ToHexString(fromLsn), Convert.ToHexString(toLsnWithTable.Lsn));
            await UpdateProcessedLsnAsync(connection, stoppingToken, toLsnWithTable.Lsn, toLsnWithTable.TableName ?? "Documents");
            return;
        }

        // Process per distinct LSN to allow safe watermarking per LSN
        var groupsByLsn = allOperations
            .GroupBy(op => Convert.ToHexString(op.StartLsn))
            .OrderBy(g => g.First().StartLsn, ByteArrayLexComparer.Instance);

        foreach (var lsnGroup in groupsByLsn)
        {
            var lsnBytes = lsnGroup.First().StartLsn;

            // Build the per-document dictionary for this LSN only
            var operationsInDocuments = new Dictionary<int, List<DocumentOperationResult>>();
            foreach (var op in lsnGroup)
            {
                if (!operationsInDocuments.TryGetValue(op.DocumentId, out var list))
                {
                    list = new List<DocumentOperationResult>();
                    operationsInDocuments[op.DocumentId] = list;
                }
                list.Add(op);
            }
            // We've got all unprocessed DB changes (operations) for supported classes in a single DB request.
            // We've grouped changes by the timestamp (LSN). There may be one or more document changes in the group.
            // In each timestamp group we've mapped DB operations to a distinct document. Each document has either 1 (INSERT or DELETE) or 2 operations (UPDATE).
            // So operationsInDocuments is a collection of document/list-of-operations.
            
            _currentLsnContext = new LsnWithTableName { Lsn = lsnBytes, TableName = toLsnWithTable.TableName };
            await ProcessDocumentBatchAsync(operationsInDocuments);

            // After successful processing of a single LSN, save watermark
            await UpdateProcessedLsnAsync(connection, stoppingToken, lsnBytes, toLsnWithTable.TableName ?? "Documents");
        }
    }
    
    private async Task<LsnWithTableName> GetLastProcessedLsnAsync(SqlConnection connection, CancellationToken stoppingToken)
    {
        var query = _queryLoader.GetQuery("GetMaxProcessedLsn");

        var result = await connection.ExecuteQueryAsync<LsnWithTableName>(
            query, 
            cancellationToken: stoppingToken);

        return result.FirstOrDefault() ?? new LsnWithTableName();
    }
    
    private async Task UpdateProcessedLsnAsync(SqlConnection connection, CancellationToken stoppingToken, byte[] lsn, string tableName)
    {
        var query = _queryLoader.GetQuery("SetProcessedLsn");

        var parameters = new 
        { 
            lsn = lsn, 
            tableName = tableName 
        };

        await connection.ExecuteNonQueryAsync(query, parameters, cancellationToken: stoppingToken);
    }
    
    private async Task<List<DocumentOperationResult>> FetchPendingChangesAsync(
        SqlConnection connection,
        CancellationToken stoppingToken,
        byte[] fromLsn,
        byte[] toLsn,
        string docClassIdsCsv)
    {
        var sqlQuery = _queryLoader.GetQuery("GetAllChangesDocumentsBatch");
        var parameters = new
        {
            fromLsn,
            toLsn,
            rowFilterOption = "all update old",
            docClassIdsCsv
        };

        var list = await connection.ExecuteQueryAsync<DocumentOperationResult>(sqlQuery, parameters, cancellationToken: stoppingToken);
        return list.ToList();
    }
    
    private async Task<LsnWithTableName> GetNextBatchLsnAsync(SqlConnection connection, CancellationToken stoppingToken, byte[] fromLsn, int batchMaxLsns, string docClassIdsCsv)
    {
        var sqlQuery = _queryLoader.GetQuery("GetToLsnForBatch");
        var parameters = new { fromLsn, batchMaxLsns, docClassIdsCsv };
        var result = await connection.ExecuteQueryAsync<LsnWithTableName>(sqlQuery, parameters, cancellationToken: stoppingToken);
        return result.FirstOrDefault() ?? new LsnWithTableName();
    }

    private sealed class ByteArrayLexComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayLexComparer Instance = new ByteArrayLexComparer();
        public int Compare(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;
            int len = Math.Min(x.Length, y.Length);
            for (int i = 0; i < len; i++)
            {
                int diff = x[i].CompareTo(y[i]);
                if (diff != 0) return diff;
            }
            return x.Length.CompareTo(y.Length);
        }
    }
    
    private async Task ProcessDocumentBatchAsync(Dictionary<int, List<DocumentOperationResult>> operationsInDocuments)
    {
        Dictionary<int, List<DocumentOperation>> mappedDictionary = operationsInDocuments
            .ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.Select(result => new DocumentOperation
                {
                    DocumentId = result.DocumentId,
                    OperationId = result.OperationId,
                    Status = result.DocPathFolderId,
                    DocClassId = result.DocClassId,
                    DocClassName = string.Empty
                }).ToList()
            );

        if (mappedDictionary.Count > 0)
        {
            _logger.LogInformation("Database operations to be processed: {Count}. LSN: {Lsn} Table: {TableName}",
                mappedDictionary.Count, _currentLsnContext.Hexadecimal, _currentLsnContext.TableName);    
        }
        await _helper.ProcessDocumentOperationSet(mappedDictionary);
    }
    
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopped CDC Service.");
        return base.StopAsync(cancellationToken);
    }
}