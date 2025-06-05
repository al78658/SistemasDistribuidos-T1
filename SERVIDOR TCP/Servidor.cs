﻿﻿﻿using Grpc.Net.Client;
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
using AnalysisService.Protos;

class Servidor
{

    static readonly Mutex ficheiroMutex1 = new();
    static readonly Mutex ficheiroMutex2 = new();
    static string serverLogFile1 = "dados_servidor1.txt";
    static string serverLogFile2 = "dados_servidor2.txt";

    static DataAnalysis.DataAnalysisClient? analysisClient;

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


        // Inicializa o cliente gRPC para análise
        analysisClient = CriarClienteAnalise();

        _ = System.Threading.Tasks.Task.Run(() => ListenForClients(listener1, serverLogFile1, ficheiroMutex1));
        _ = System.Threading.Tasks.Task.Run(() => ListenForClients(listener2, serverLogFile2, ficheiroMutex2));

        // Manter o programa em execução
        Console.ReadLine();

    }

    static DataAnalysis.DataAnalysisClient CriarClienteAnalise()
    {
        var httpHandler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            SslOptions = { RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true }
        };
        var grpcChannelOptions = new GrpcChannelOptions
        {
            HttpHandler = httpHandler,
            Credentials = ChannelCredentials.Insecure
        };
        var channel = GrpcChannel.ForAddress("http://localhost:7275", grpcChannelOptions);
        return new DataAnalysis.DataAnalysisClient(channel);
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
            
            // Increase buffer size to handle larger JSON payloads
            byte[] buffer = new byte[8192]; // Increased from 2048 to 8192
            
            // Read all available data from the stream
            using MemoryStream ms = new MemoryStream();
            int bytesRead;
            
            do {
                bytesRead = stream.Read(buffer, 0, buffer.Length);
                ms.Write(buffer, 0, bytesRead);
            } while (stream.DataAvailable);
            
            string message = Encoding.UTF8.GetString(ms.ToArray());
            
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

                // Tenta converter o conteudo em uma lista de double para análise
                if (analysisClient != null)
                {
                    var valores = new System.Collections.Generic.List<double>();
                    foreach (var s in conteudo.Split(';', ',', ' '))
                    {
                        if (double.TryParse(s, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double v))
                            valores.Add(v);
                    }
                    if (valores.Count > 0)
                    {
                        var req = new DataRequest { Source = id };
                        req.Values.AddRange(valores);
                        try
                        {
                            var result = analysisClient.AnalyzeAsync(req).GetAwaiter().GetResult();
                            Console.WriteLine($"[ANÁLISE] Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Erro ao chamar análise RPC: {ex.Message}");
                        }
                    }
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