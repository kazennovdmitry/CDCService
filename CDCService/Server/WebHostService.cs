using CDC.CoreLogic.Interfaces;
using CDC.CoreLogic.Models;

namespace CDC.CDCService.Server;

public class WebHostService : IHostedService
{
    private readonly WebApplication _webApp;
    private readonly ILogger<WebHostService> _logger;
    private readonly IMainHelper _helper;
    private readonly Task _runTask;

    public WebHostService(WebApplicationBuilder builder, 
        IMainHelper helper,
        ILogger<WebHostService> logger,
        int port)
    {
        _helper =  helper;
        _logger = logger;
        _webApp = builder.Build();
        _webApp.Urls.Add($"http://0.0.0.0:{port}");

        // Health check
        _webApp.MapGet("/", () => "CDC Service is running");

        // Generate test message and interact with repository + publisher
        _webApp.MapPost("/publish", async (PublishRequest request) =>
        {
            var result = await TestDocumentOperations(request);
            return Results.Ok($"Published messages: {result}");
        });

        _runTask = _webApp.RunAsync();
    }

    private async Task<string> TestDocumentOperations(PublishRequest request)
    {
        List<DocumentOperation> operations = request.Operations.Select
        (op => new DocumentOperation
            {
                DocumentId = request.DocumentId,
                OperationId = op.OperationId,
                Status = op.DocPathFolderId,
                DocClassId = op.DocClassId,
                DocClassName = string.Empty
            }
        ).ToList();
        KeyValuePair<int, List<DocumentOperation>> operationsInDocument = new(request.DocumentId, operations);
        return await _helper.ProcessDocumentOperations(operationsInDocument);
    }

    public Task StartAsync(CancellationToken cancellationToken) => _runTask;

    public async Task StopAsync(CancellationToken cancellationToken) => await _webApp.StopAsync();
}