using CDC.CoreLogic.Models;

namespace CDC.CoreLogic.Interfaces;

public interface IPublisherService
{
    public Task<string> PublishMessageWithCustomAttributesAsync(EventMessage message);
}