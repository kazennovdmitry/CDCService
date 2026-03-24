using Microsoft.Extensions.Logging;
using System.Text;
using CDC.CoreLogic.Interfaces;
using CDC.CoreLogic.Models;

namespace CDC.CoreLogic;

public class MainHelper(
    ILogger<MainHelper> logger,
    IPublisherService publisherService,
    IRepositoryManager repositoryManager)
    : IMainHelper
{
    private Dictionary<int, string?> _paymentDocClasses = new();

    public Dictionary<int, string> PaymentDocClasses
    {
        set => _paymentDocClasses = value;
        get => _paymentDocClasses;
    }

    public async Task ProcessDocumentOperationSet(Dictionary<int, List<DocumentOperation>> operationsInDocuments)
    {
        foreach (var operationsInDocument in operationsInDocuments)
        {
            await ProcessDocumentOperations(operationsInDocument);
        }
    }

    public async Task<string> ProcessDocumentOperations(KeyValuePair<int, List<DocumentOperation>> operationsInDocument)
    {
        var result = string.Empty;
        var operations = operationsInDocument.Value;
        
        if (operations.Count == 1)
        {
            // Only 1 row per an operation. This is either Insert or delete. See CDC documentation.
            var operation = operations.First();
            if (operation.OperationId == 1)
            {
                // delete operation
                logger.LogTrace($"Document with ID {operation.DocumentId} was DELETED. {Environment.NewLine} " +
                                 $"Status was {operation.Status}; {Environment.NewLine} " +
                                 $"DocClass was {operation.DocClassId};");
            }
            else if (operation.OperationId == 2)
            {
                // insert operation
                logger.LogTrace($"Document with ID {operation.DocumentId} was INSERTED. {Environment.NewLine} " +
                                       $"Status is {operation.Status}; {Environment.NewLine} " +
                                       $"DocClass is {operation.DocClassId};");
            }
        }
        else if (operations.Count == 2)
        {
            // Update.
            var updateOldValues = operations.First(o => o.OperationId == 3);
            var updateNewValues = operations.First(o => o.OperationId == 4);
            logger.LogTrace($"Document with ID {updateOldValues.DocumentId} was UPDATED. {Environment.NewLine}" +
                             $"Status was {updateOldValues.Status}, now is {updateNewValues.Status}; {Environment.NewLine}" +
                             $"DocClass was {updateOldValues.DocClassId}, now is {updateNewValues.DocClassId};");
            
            result = await ProcessDocumentUpdate(updateOldValues, updateNewValues);
        }
        
        return result;
    }

    private async Task<string> ProcessDocumentUpdate(DocumentOperation updateOldValues, DocumentOperation updateNewValues)
    {
        var result = string.Empty;
        
        if (PaymentDocClasses.TryGetValue(updateOldValues.DocClassId, out string? shortName)
            && updateOldValues.Status != updateNewValues.Status)
        {
            updateOldValues.DocClassName = updateNewValues.DocClassName = shortName;
            result = await ProcessPaymentUpdate(updateOldValues, updateNewValues);
        }

        return result;
    }

    private async Task<string> ProcessPaymentUpdate(DocumentOperation updateOldValues, DocumentOperation updateNewValues)
    {
        var sb = new StringBuilder();
        bool first = true;
        
        var payloadsWithAttributes = updateOldValues.DocClassName switch
        {
            "IncomingPayment" => await repositoryManager.GetTransfersIncoming(updateNewValues),
            "OutgoingPayment" => await repositoryManager.GetTransfersOutgoing(updateNewValues),
            "PaymentInternal" => await repositoryManager.GetTransferInternal(updateNewValues),
            _ => null
        };
        
        foreach(var payloadWithAttributes in payloadsWithAttributes!)
        {
            var messageId = await PrepareAndSendEvent(payloadWithAttributes); 
            if (!first) sb.Append(", ");
            sb.Append(messageId);
            first = false;
        }

        return sb.ToString();
    }

    private async Task<string> PrepareAndSendEvent(ApiPayloadWithAttributes payloadWithAttributes)
    {
        var message = new EventMessage()
        {
            Data = Convert.ToBase64String(Encoding.UTF8.GetBytes(payloadWithAttributes.Payload)),
            Attributes = payloadWithAttributes.Attributes
        };

        return await Publish(message);
    }

    private async Task<string> Publish(EventMessage message)
    {
        return await publisherService.PublishMessageWithCustomAttributesAsync(message);
    }
}