namespace CDC.CoreLogic.Models;

public class Transfer(int documentId)
{
    public int DocumentId { get; } = documentId;
    public int DebitClientId;
    public int DebitAccountId;
    public int CreditClientId;
    public int CreditAccountId;
}