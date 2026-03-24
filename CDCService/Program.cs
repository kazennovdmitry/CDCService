using CDC.CoreLogic;
using CDC.CoreLogic.Interfaces;
using CDC.Kafka;
using CDC.Repository;
using CDC.CDCService.Server;

namespace CDC.CDCService;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(
            new HostApplicationBuilderSettings
            {
                Args = args,
                ContentRootPath = ContentRootPath(),
                EnvironmentName =
                    Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                    ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT"),
            }
        );
        
        builder.Services.AddHostedService<CdcWorker>();
        builder.Services.AddSingleton<IPublisherService, KafkaPublisherService>();
        builder.Services.AddSingleton<IMainHelper, MainHelper>();
        builder.Services.AddSingleton<IRepositoryManager, RepositoryManager>();
        GetWebHostService(builder);
        
        var host = builder.Build();
        host.Run();
    }
    
    private static void GetWebHostService(HostApplicationBuilder builder)
    {
        var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
        builder.Services.AddSingleton<IHostedService>(serviceProvider =>
        {
            var webHostBuilder = WebApplication.CreateBuilder();
            var logger = serviceProvider.GetRequiredService<ILogger<WebHostService>>();
            var helper = serviceProvider.GetRequiredService<IMainHelper>();

            return new WebHostService(webHostBuilder, helper, logger, int.Parse(port));
        });
    }
    
    private static string? ContentRootPath()
    {
        var contentRootPath = Environment.GetEnvironmentVariable("ASPNETCORE_CONTENTROOT");
        contentRootPath = !string.IsNullOrWhiteSpace(contentRootPath)
            ? contentRootPath
            : Path.GetDirectoryName(Environment.GetCommandLineArgs()[0]);
        Directory.SetCurrentDirectory(Path.GetDirectoryName(Environment.GetCommandLineArgs()[0])!);
        return contentRootPath;
    }
}