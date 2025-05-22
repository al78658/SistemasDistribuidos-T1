using AnalysisService.Services;

var builder = WebApplication.CreateBuilder(args);

// Configurar Kestrel para HTTP/2 explícito
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(7275, listenOptions =>
    {
        listenOptions.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
    });
});

// Add services to the container.
builder.Services.AddGrpc();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<GreeterService>();
app.MapGrpcService<DataAnalysisService>();
app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

app.Run();