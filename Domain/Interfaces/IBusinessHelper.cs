using CDC.CoreLogic.Models;

namespace CDC.CoreLogic.Interfaces;

public interface IMainHelper
{
    /// <summary>
    /// Processes all operations for all documents.
    /// </summary>
    /// <remarks> CDC OperationId: 1 = DELETE, 2 = INSERT, 3 = UPDATE old values, 4 = UPDATE new values. </remarks>
    public Task ProcessDocumentOperationSet(Dictionary<int, List<DocumentOperation>> operationsInDocuments);

    /// <summary>
    /// Processes all operations for a single document.
    /// </summary>
    /// <remarks> CDC OperationId: 1 = DELETE, 2 = INSERT, 3 = UPDATE old values, 4 = UPDATE new values. </remarks>
    public Task<string> ProcessDocumentOperations(KeyValuePair<int, List<DocumentOperation>> operationsInDocument);

    public Dictionary<int, string> PaymentDocClasses { get; set; }
}