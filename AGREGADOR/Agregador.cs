﻿using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Net.Client;
using AGREGADOR;

class Agregador
{
    static Dictionary<string, List<string>> bufferWavy = new();
    static Dictionary<string, WavyConfig> wavyConfigs = new();
    static Dictionary<string, WavyStatus> wavyStatus = new();
    static readonly object fileLock = new();
    static readonly object statusLock = new();
    static readonly object configLock = new();

    static readonly List<string> ColunasDados = new()
    {
        "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST"
    };

    static void Main(string[] args)
    {
        LoadConfigurations();

        string serverIp = "127.0.0.1";


        // Iniciar subscrição RabbitMQ
        Task.Run(() => IniciarRabbitMqSubscriber());

        // TCP legacy (comentado, não usar mais)
        /*
        IniciarAgregador(7001, serverIp, 6000);
        IniciarAgregador(7002, serverIp, 6000);
        IniciarAgregador(7003, serverIp, 6001);
        */

        // gRPC
        Task.Run(async () => 
        {
            await Task.Delay(2000);
            await IniciarRcpAsync();
        });

        Console.WriteLine("AGREGADOR iniciado e subscrevendo tópicos RabbitMQ.");
        Console.WriteLine("Pressiona Ctrl+C para terminar.");

        while (true)
            Thread.Sleep(1000);
    }

    static void IniciarRabbitMqSubscriber()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        channel.ExchangeDeclare(exchange: "wavy_data", type: ExchangeType.Topic);

        string[] topics = { "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST" };
        var queueName = channel.QueueDeclare().QueueName;
        foreach (var topic in topics)
            channel.QueueBind(queue: queueName, exchange: "wavy_data", routingKey: topic);

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var topic = ea.RoutingKey;
            Console.WriteLine($"[AGREGADOR][RabbitMQ] Recebido do tópico {topic}: {message}");
            // Aqui pode processar e agregar os dados conforme necessário
            // Exemplo: bufferizar por tópico
            lock (bufferWavy)
            {
                if (!bufferWavy.ContainsKey(topic))
                    bufferWavy[topic] = new List<string>();
                bufferWavy[topic].Add(message);
            }
        };
        channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);

        // Mantém o consumidor ativo
        while (true) Thread.Sleep(1000);
    }

    static void IniciarAgregador(int port, string serverIp, int serverPort)
    {
        TcpListener listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"AGREGADOR a escutar na porta {port}");

        Task.Run(async () =>
        {
            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                // Executar HandleClient de forma assíncrona sem bloquear o loop principal
                _ = Task.Run(() => HandleClient(client, serverIp, serverPort));
            }
        });
        // Removido a chamada ao IniciarRcp() para cada instância do Agregador
    }

    // Método modificado para ser chamado apenas uma vez no Main
    static async Task IniciarRcpAsync()
    {
        Console.WriteLine("Tentando conectar ao serviço gRPC em https://localhost:7177...");
        
        try
        {
            // Verificar se o serviço está acessível antes de tentar a conexão gRPC
            using (var tcpClient = new TcpClient())
            {
                try
                {
                    // Tentar conectar com timeout de 2 segundos
                    var connectTask = tcpClient.ConnectAsync("localhost", 7177);
                    var timeoutTask = Task.Delay(2000);
                    
                    if (await Task.WhenAny(connectTask, timeoutTask) == timeoutTask)
                    {
                        throw new TimeoutException("Timeout ao tentar conectar ao serviço gRPC");
                    }
                    
                    Console.WriteLine("Porta 7177 está acessível, tentando estabelecer conexão gRPC...");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"AVISO: Porta 7177 não está acessível: {ex.Message}");
                    Console.WriteLine("Verifique se o serviço PreProcessingService está em execução.");
                    Console.WriteLine("O sistema continuará funcionando sem o serviço gRPC.");
                    return;
                }
            }
            
            var httpHandler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (HttpRequestMessage, cert, chain, sslPolicyErrors) => true
            };

            // Configurar timeout mais curto para evitar bloqueios longos
            var httpClient = new HttpClient(httpHandler)
            {
                Timeout = TimeSpan.FromSeconds(5)
            };

            using var channel = GrpcChannel.ForAddress("https://localhost:7177", new GrpcChannelOptions
            {
                HttpHandler = httpHandler,
                // Adicionar configurações de timeout para o canal gRPC
                MaxReceiveMessageSize = 4 * 1024 * 1024, // 4MB
                MaxSendMessageSize = 4 * 1024 * 1024     // 4MB
            });

            var client = new AGREGADOR.Greeter.GreeterClient(channel);
            
            // Usar um timeout para a chamada gRPC
            var callOptions = new Grpc.Core.CallOptions(deadline: DateTime.UtcNow.AddSeconds(5));
            var reply = await client.SayHelloAsync(
                              new HelloRequest { Name = "AGREGADOR" }, callOptions);
            
            Console.WriteLine("Conexão gRPC estabelecida com sucesso!");
            Console.WriteLine("Greeting: " + reply.Message);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"AVISO: Não foi possível conectar ao serviço gRPC: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Causa: {ex.InnerException.Message}");
            }
            Console.WriteLine("O sistema continuará funcionando sem o serviço gRPC.");
            Console.WriteLine("Para resolver este problema:");
            Console.WriteLine("1. Verifique se o serviço PreProcessingService está em execução");
            Console.WriteLine("2. Verifique se a porta 7177 está disponível e não bloqueada por firewall");
            Console.WriteLine("3. Execute o PreProcessingService com o comando: dotnet run --project PreProcessingService");
        }
    }
    static void LoadConfigurations()
    {
        if (File.Exists("config_wavy.txt"))
        {
            foreach (var line in File.ReadLines("config_wavy.txt"))
            {
                var parts = line.Split(':');
                if (parts.Length < 4) continue;

                string wavyId = parts[0].ToLower();
                wavyConfigs[wavyId] = new WavyConfig
                {
                    PreProcessamento = parts[1],
                    VolumeDadosEnviar = int.Parse(parts[2]),
                    ServidorAssociado = parts[3]
                };
            }
        }

        RecarregarStatusWavy();
    }

    static async Task HandleClient(TcpClient client, string serverIp, int serverPort)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[2048];
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            string message = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();

            if (string.IsNullOrWhiteSpace(message)) return;

            var json = JsonSerializer.Deserialize<JsonElement>(message);
            string type = json.GetProperty("type").GetString();

            switch (type)
            {
                case "START":
                    var localPort = ((IPEndPoint)client.Client.LocalEndPoint).Port;
                    SendResponse(stream, new { type = "ACK", ip = "127.0.0.1", port = localPort });
                    break;

                case "REGISTER":
                    string id = json.GetProperty("id").GetString().ToLower();

                    bufferWavy[id] = new List<string>();
                    wavyStatus[id] = new WavyStatus
                    {
                        Status = "associada",
                        DataTypes = new List<string>(),
                        LastSync = DateTime.Now
                    };
                    AtualizarEstadoWavy(id, "associada");

                    if (!wavyConfigs.ContainsKey(id))
                    {
                        wavyConfigs[id] = new WavyConfig
                        {
                            PreProcessamento = "nenhum",
                            VolumeDadosEnviar = 5,
                            ServidorAssociado = serverIp
                        };
                        AtualizarConfigWavy(id);
                    }

                    // Recarregar o estado da WAVY após o registro
                    RecarregarStatusWavy();

                    SendResponse(stream, new { type = "ACK", message = $"ID {id} registado." });
                    Thread.Sleep(1000);

                    wavyStatus[id].DataTypes = new List<string>(ColunasDados);
                    AtualizarEstadoWavy(id, "operacao");
                    SendResponse(stream, new { type = "STATUS", status = "operacao" });
                    break;

                case "DATA":
                    string wavyId = json.GetProperty("id").GetString().ToLower();
                    string conteudo = json.GetProperty("conteudo").GetString();

                    // Garantir que o buffer exista
                    if (!bufferWavy.ContainsKey(wavyId))
                    {
                        bufferWavy[wavyId] = new List<string>();
                    }

                    // Recarregar configurações do arquivo antes de processar os dados
                    RecarregarConfiguracaoWavy(wavyId);

                    RecarregarStatusWavy(); // <-- Atualiza estado a partir do ficheiro

                    lock (statusLock)
                    {
                        if (wavyStatus[wavyId].Status == "desativada")
                        {
                            SendResponse(stream, new { type = "NOTIFICACAO", message = "WAVY desativada. Encerrando envio." });
                            return;
                        }

                        if (wavyStatus[wavyId].Status == "manutencao")
                        {
                            SendResponse(stream, new { type = "NOTIFICACAO", message = "WAVY em manutencao. Dados descartados." });
                            return;
                        }
                    }

                    string preproc = wavyConfigs.ContainsKey(wavyId) ? wavyConfigs[wavyId].PreProcessamento : "nenhum";
                    conteudo = PreProcessar(conteudo, preproc);

                    if (conteudo == null)
                    {
                        SendResponse(stream, new { type = "ACK", message = "Dados invalidos descartados." });
                        return;
                    }

                    bufferWavy[wavyId].Add(conteudo);
                    Console.WriteLine($"[BUFFER] {wavyId}: {bufferWavy[wavyId].Count}/{GetVolume(wavyId)} armazenados.");

                    SendResponse(stream, new { type = "ACK", message = "Dados recebidos." });

                    if (bufferWavy[wavyId].Count >= GetVolume(wavyId))
                        await SendBufferedData(serverIp, serverPort, wavyId);

                    break;



                case "MAINTENANCE":
                    string maintenanceId = json.GetProperty("id").GetString().ToLower();
                    AtualizarEstadoWavy(maintenanceId, "manutencao");
                    SendResponse(stream, new { type = "ACK", message = $"WAVY {maintenanceId} em manutencao." });
                    break;

                case "END":
                    string endId = json.GetProperty("id").GetString().ToLower();
                    await SendBufferedData(serverIp, serverPort, endId);
                    AtualizarEstadoWavy(endId, "desativada");
                    SendResponse(stream, new { type = "ACK", message = $"Sessao terminada para {endId}." });
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Erro: " + ex.Message);
        }
        finally
        {
            client.Close();
        }
    }

    static void RecarregarConfiguracaoWavy(string wavyId)
    {
        lock (configLock)
        {
            if (File.Exists("config_wavy.txt"))
            {
                foreach (var line in File.ReadLines("config_wavy.txt"))
                {
                    var parts = line.Split(':');
                    if (parts.Length < 4) continue;

                    string id = parts[0].ToLower();
                    if (id == wavyId)
                    {
                        wavyConfigs[id] = new WavyConfig
                        {
                            PreProcessamento = parts[1],
                            VolumeDadosEnviar = int.Parse(parts[2]),
                            ServidorAssociado = parts[3]
                        };
                        break;
                    }
                }
            }
        }
    }
    static void RecarregarStatusWavy()
    {
        lock (statusLock)
        {
            if (File.Exists("status.txt"))
            {
                foreach (var line in File.ReadLines("status.txt"))
                {
                    var parts = line.Split(':');
                    if (parts.Length < 4) continue;

                    string wavyId = parts[0].ToLower();
                    var dataTypes = parts[2].Trim('[', ']').Split(',', StringSplitOptions.RemoveEmptyEntries);
                    DateTime.TryParseExact(parts[3], "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime lastSync);

                    wavyStatus[wavyId] = new WavyStatus
                    {
                        Status = parts[1],
                        DataTypes = new List<string>(dataTypes),
                        LastSync = lastSync
                    };

                    if (!bufferWavy.ContainsKey(wavyId))
                        bufferWavy[wavyId] = new List<string>();
                }
            }
        }

    }

    static void SendResponse(NetworkStream stream, object response)
    {
        string jsonResponse = JsonSerializer.Serialize(response);
        byte[] resp = Encoding.UTF8.GetBytes(jsonResponse);
        stream.Write(resp, 0, resp.Length);
        Console.WriteLine($"[RESPOSTA ENVIADA] {jsonResponse}");
    }

    static int GetVolume(string id)
    {
        return wavyConfigs.ContainsKey(id) ? wavyConfigs[id].VolumeDadosEnviar : 5;
    }

    static string PreProcessar(string data, string tipo)
    {
        return tipo switch
        {
            "trim" => data.Trim(),
            "validar_corrigir" => ValidarECorrigir(data),
            "remover_virgulas" => RemoverVirgulas(data),
            "nenhum" => data,
            _ => data
        };
    }


    static string RemoverVirgulas(string linhaCsv)
    {
        var partes = linhaCsv.Split('|');
        for (int i = 0; i < partes.Length; i++)
        {
            var valores = partes[i].Split(',');

            if (valores.Length > 1)
            {
                string timestamp = valores[0];
                string[] measurements = valores[1..];

                for (int j = 0; j < measurements.Length; j++)
                    measurements[j] = measurements[j].Replace(',', ';');

                string novaParte = timestamp + " " + string.Join(";", measurements);
                partes[i] = novaParte;
            }
        }
        return string.Join(" | ", partes);
    }

    static string ValidarECorrigir(string linhaCsv)
    {
        try
        {
            var campos = linhaCsv.Split(',');
            if (campos.Length < 6) return null;

            for (int i = 0; i < campos.Length; i++)
            {
                campos[i] = campos[i].Trim();

                if (double.TryParse(campos[i], NumberStyles.Any, CultureInfo.InvariantCulture, out double valor) && valor < 0)
                    campos[i] = "0";
            }

            return string.Join(",", campos);
        }
        catch
        {
            return null;
        }
    }

    static async Task SendBufferedData(string ip, int port, string id)
    {
        if (bufferWavy.ContainsKey(id) && bufferWavy[id].Count > 0)
        {
            string conteudo = string.Join(" | ", bufferWavy[id]);
            bufferWavy[id].Clear();

            // Chamada gRPC para pré-processamento remoto
            string processedData = null;
            
            // Verificar se devemos tentar usar o serviço gRPC
            bool useGrpc = false;
            
            // Verificar rapidamente se o serviço está acessível
            using (var tcpClient = new TcpClient())
            {
                try
                {
                    var connectTask = tcpClient.ConnectAsync("localhost", 7177);
                    var timeoutTask = Task.Delay(500); // Timeout curto para não atrasar muito
                    
                    if (await Task.WhenAny(connectTask, timeoutTask) == connectTask)
                    {
                        useGrpc = true;
                    }
                }
                catch
                {
                    // Ignorar erros e continuar sem gRPC
                }
            }
            
            if (useGrpc)
            {
                try
                {
                    var httpHandler = new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true
                    };
                    
                    // Configurar timeout mais curto para evitar bloqueios longos
                    var httpClient = new HttpClient(httpHandler)
                    {
                        Timeout = TimeSpan.FromSeconds(3)
                    };
                    
                    using var channel = Grpc.Net.Client.GrpcChannel.ForAddress("https://localhost:7177", 
                        new Grpc.Net.Client.GrpcChannelOptions 
                        { 
                            HttpHandler = httpHandler,
                            // Adicionar configurações de timeout para o canal gRPC
                            MaxReceiveMessageSize = 4 * 1024 * 1024, // 4MB
                            MaxSendMessageSize = 4 * 1024 * 1024     // 4MB
                        });
                    
                    var grpcClient = new PreProcessingService.Protos.PreProcessing.PreProcessingClient(channel);
                    var preprocType = wavyConfigs.ContainsKey(id) ? wavyConfigs[id].PreProcessamento : "nenhum";
                    var grpcRequest = new PreProcessingService.Protos.PreProcessRequest { WavyId = id, RawData = conteudo };
                    
                    // Usar um timeout para a chamada gRPC
                    var callOptions = new Grpc.Core.CallOptions(deadline: DateTime.UtcNow.AddSeconds(3));
                    var grpcResponse = grpcClient.PreProcess(grpcRequest, callOptions);
                    processedData = grpcResponse.ProcessedData;
                    
                    Console.WriteLine($"[INFO] Dados processados com sucesso pelo serviço gRPC para {id}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[AVISO] Falha ao chamar o serviço de pré-processamento gRPC: {ex.Message}");
                    Console.WriteLine("[INFO] Usando dados sem pré-processamento remoto.");
                    // Em vez de retornar, vamos usar os dados originais
                    processedData = conteudo;
                }
            }
            else
            {
                // Usar pré-processamento local
                string preproc = wavyConfigs.ContainsKey(id) ? wavyConfigs[id].PreProcessamento : "nenhum";
                processedData = PreProcessar(conteudo, preproc);
                Console.WriteLine($"[INFO] Usando pré-processamento local ({preproc}) para {id}");
            }

            if (string.IsNullOrWhiteSpace(processedData))
            {
                Console.WriteLine($"[ERRO] Conteúdo inválido após pré-processamento remoto para {id}");
                return;
            }

            Console.WriteLine($"[ENVIANDO PARA SERVIDOR] {id}: {processedData}");

            try
            {
                var payload = new { type = "FORWARD", data = new { id, conteudo = processedData } };
                string json = JsonSerializer.Serialize(payload);
                byte[] buffer = Encoding.UTF8.GetBytes(json);

                using TcpClient client = new TcpClient(ip, port);
                using NetworkStream stream = client.GetStream();
                stream.Write(buffer, 0, buffer.Length);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao enviar ao servidor: " + ex.Message);
            }
        }
    }

    static void AtualizarEstadoWavy(string wavyId, string novoStatus)
    {
        wavyId = wavyId.ToLower();

        if (wavyStatus.ContainsKey(wavyId))
        {
            wavyStatus[wavyId].Status = novoStatus;
            wavyStatus[wavyId].LastSync = DateTime.Now;
        }
        else
        {
            wavyStatus[wavyId] = new WavyStatus
            {
                Status = novoStatus,
                DataTypes = new List<string>(),
                LastSync = DateTime.Now
            };
        }

        AtualizarStatusTxt(wavyId, novoStatus);
    }

    static void AtualizarStatusTxt(string wavyId, string novoStatus)
    {
        lock (statusLock)
        {
            var status = wavyStatus[wavyId];
            string tipos = "[" + string.Join(",", status.DataTypes) + "]";
            string linha = $"{wavyId}:{novoStatus}:{tipos}:{status.LastSync:yyyy-MM-dd HH:mm:ss}";

            var linhas = new List<string>(File.ReadAllLines("status.txt"));
            bool linhaAtualizada = false;

            for (int i = 0; i < linhas.Count; i++)
            {
                if (linhas[i].StartsWith(wavyId + ":"))
                {
                    linhas[i] = linha;
                    linhaAtualizada = true;
                    break;
                }
            }

            if (!linhaAtualizada)
                linhas.Add(linha);

            File.WriteAllLines("status.txt", linhas);

            // Recarregar o estado em memória após atualizar o arquivo
            RecarregarStatusWavy();
        }
        Console.WriteLine($"Estado da WAVY {wavyId} ({novoStatus}) atualizado no ficheiro status.txt");
    }


    static void AtualizarConfigWavy(string wavyId)
    {
        lock (configLock)
        {
            var config = wavyConfigs[wavyId];
            string linha = $"{wavyId}:{config.PreProcessamento}:{config.VolumeDadosEnviar}:{config.ServidorAssociado}";

            var linhas = new List<string>(File.ReadAllLines("config_wavy.txt"));
            bool linhaAtualizada = false;

            for (int i = 0; i < linhas.Count; i++)
            {
                if (linhas[i].StartsWith(wavyId + ":"))
                {
                    linhas[i] = linha;
                    linhaAtualizada = true;
                    break;
                }
            }

            if (!linhaAtualizada)
                linhas.Add(linha);

            File.WriteAllLines("config_wavy.txt", linhas);
        }
        Console.WriteLine($"Configuração da WAVY {wavyId} atualizada no ficheiro config_wavy.txt");
    }
}

class WavyConfig
{
    public string PreProcessamento { get; set; }
    public int VolumeDadosEnviar { get; set; }
    public string ServidorAssociado { get; set; }
}

class WavyStatus
{
    public string Status { get; set; }
    public List<string> DataTypes { get; set; }
    public DateTime LastSync { get; set; }
}