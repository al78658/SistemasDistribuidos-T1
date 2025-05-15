using Grpc.Net.Client;
using Grpc.Core;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using SERVIDOR_TCP;

class Servidor
{
    static readonly Mutex ficheiroMutex1 = new();
    static readonly Mutex ficheiroMutex2 = new();
    static string serverLogFile1 = "dados_servidor1.txt";
    static string serverLogFile2 = "dados_servidor2.txt";

    static void Main(string[] args)
    {
        int port1 = 6000;
        int port2 = 6001;

        TcpListener listener1 = new TcpListener(IPAddress.Any, port1);
        TcpListener listener2 = new TcpListener(IPAddress.Any, port2);

        listener1.Start();
        listener2.Start();

        Console.WriteLine("SERVIDOR 1 a escutar na porta 6000...");
        Console.WriteLine("SERVIDOR 2 a escutar na porta 6001...");


        _ = System.Threading.Tasks.Task.Run(() => ListenForClients(listener1, serverLogFile1, ficheiroMutex1));
        _ = System.Threading.Tasks.Task.Run(() => ListenForClients(listener2, serverLogFile2, ficheiroMutex2));

        // Manter o programa em execução
        Console.ReadLine();

    }

    static async void ligarRpc()
    {
        var httpHandler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true, // Garante suporte a múltiplas conexões HTTP/2
            SslOptions = { RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true } // Aceita qualquer certificado
        };

        var grpcChannelOptions = new GrpcChannelOptions
        {
            HttpHandler = httpHandler,
            Credentials = ChannelCredentials.Insecure // Força o uso de HTTP/2 sem SSL para desenvolvimento
        };

        using var channel = GrpcChannel.ForAddress("http://localhost:7275", grpcChannelOptions);
        var client = new SERVIDOR_TCP.Greeter.GreeterClient(channel);

        try
        {
            var reply = await client.SayHelloAsync(
                              new HelloRequest { Name = "SERVIDOR_TCP" });
            Console.WriteLine("Greeting: " + reply.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao conectar ao servidor gRPC: " + ex.Message);
        }
    }
    static void ListenForClients(TcpListener listener, string logFile, Mutex mutex)
    {
        ligarRpc();

        while (true)
        {
            TcpClient client = listener.AcceptTcpClient();
            _ = System.Threading.Tasks.Task.Run(() => HandleClient(client, logFile, mutex));
        }
    }

    static void HandleClient(TcpClient client, string logFile, Mutex mutex)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[2048];
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            var json = JsonSerializer.Deserialize<JsonElement>(message);
            if (json.GetProperty("type").GetString() == "FORWARD")
            {
                var dataElem = json.GetProperty("data");
                string id = dataElem.GetProperty("id").GetString();
                string conteudo = dataElem.GetProperty("conteudo").GetString();

                Console.WriteLine($"[RECEBIDO] {id}: {conteudo}");

                mutex.WaitOne();
                try
                {
                    File.AppendAllText(logFile, $"{DateTime.Now:HH:mm:ss} | {id} | {conteudo}\n");
                }
                finally
                {
                    mutex.ReleaseMutex();
                }
            }

            stream.Close();
            client.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro ao processar ligação: " + ex.Message);
        }
    }
}
