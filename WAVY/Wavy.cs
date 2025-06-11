using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Linq;
using System.Collections.Generic;

class Wavy
{
    const int delay = 2000; // Tempo entre envios (2 segundos)
    const string PROGRESS_PREFIX = "progress_";

    static Dictionary<string, Mutex> mutexPorAgregador = new(); // Um mutex por agregador
    static readonly object mutexLock = new(); // Lock para acesso à criação de mutex
    static readonly Mutex globalMutex = new(); // Mutex global para sincronizar o envio de dados

    static void Main(string[] args)
    {
        string aggregatorConfigPath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\..\\..\\AGREGADOR\\bin\\Debug\\net8.0"));

        if (!Directory.Exists(aggregatorConfigPath))
        {
            Console.WriteLine($"Pasta do Agregador não encontrada: {aggregatorConfigPath}");
            return;
        }

        string[] configFiles = Directory
            .GetFiles(aggregatorConfigPath, "config_wavy*.txt")
            .Where(f =>
            {
                string fileName = Path.GetFileName(f);
                return fileName.Length > "config_wavy".Length && char.IsDigit(fileName["config_wavy".Length]);
            })
            .ToArray();

        if (configFiles.Length == 0)
        {
            Console.WriteLine("Nenhum ficheiro de configuração válido encontrado.");
            return;
        }

        while (true)
        {
            Console.WriteLine("\n==== MENU WAVY ====");
            Console.WriteLine("1. Verificar estado de uma WAVY");
            Console.WriteLine("2. Escolher WAVYs para enviar dados");
            Console.WriteLine("0. Sair");
            Console.Write("Escolha uma opção: ");

            string input = Console.ReadLine();
            if (input.Trim() == "0")
            {
                Console.WriteLine("Encerrando o programa.");
                return;
            }
            else if (input.Trim() == "1")
            {
                VerificarEstadoWavy();
                continue;
            }
            else if (input.Trim() == "2")
            {
                EscolherWavysParaEnviarDados(configFiles);
                continue;
            }
            else
            {
                Console.WriteLine("Opção inválida. Tente novamente.");
            }
        }
    }

    static void VerificarEstadoWavy()
    {
        Console.Clear();
        Console.Write("Digite o ID da WAVY: ");
        string wavyId = Console.ReadLine()?.Trim().ToLower();

        if (string.IsNullOrEmpty(wavyId))
        {
            Console.WriteLine("ID da WAVY inválido.");
            return;
        }

        string statusFilePath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\..\\..\\AGREGADOR\\bin\\Debug\\net8.0\\status.txt"));
        if (!File.Exists(statusFilePath))
        {
            Console.WriteLine("Arquivo status.txt não encontrado.");
            return;
        }

        string[] lines = File.ReadAllLines(statusFilePath);
        foreach (string line in lines)
        {
            string[] parts = line.Split(':');
            if (parts.Length >= 4 && parts[0].ToLower() == wavyId)
            {
                string status = parts[1];
                Console.WriteLine($"ID: {wavyId}, Estado: {status}, Última Sincronização: {parts[3]}");
                return;
            }
        }

        Console.WriteLine($"WAVY {wavyId} não encontrada no arquivo status.txt.");
    }

    static void EscolherWavysParaEnviarDados(string[] configFiles)
    {
        Console.Clear();

        // Recarregar a lista de arquivos de configuração
        string aggregatorConfigPath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\..\\..\\AGREGADOR\\bin\\Debug\\net8.0"));
        configFiles = Directory
            .GetFiles(aggregatorConfigPath, "config_wavy*.txt")
            .Where(f =>
            {
                string fileName = Path.GetFileName(f);
                return fileName.Length > "config_wavy".Length && char.IsDigit(fileName["config_wavy".Length]);
            })
            .ToArray();

        Console.WriteLine("Escolha as WAVYs que deseja iniciar (separadas por espaço, exp: 1 2):");

        for (int i = 0; i < configFiles.Length; i++)
        {
            Console.WriteLine($"{i + 1}. {Path.GetFileName(configFiles[i])}");
        }

        Console.Write("Escolha as opções: ");
        string input = Console.ReadLine();
        string[] selectedOptions = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        List<Thread> wavyThreads = new();

        foreach (string opt in selectedOptions)
        {
            if (int.TryParse(opt, out int index) && index >= 1 && index <= configFiles.Length)
            {
                string configFile = configFiles[index - 1];
                Thread t = new Thread(() => RunWavyFromConfig(configFile));
                t.Start();
                wavyThreads.Add(t);
            }
            else
            {
                Console.WriteLine($"Opção inválida: {opt}");
            }
        }

        foreach (var thread in wavyThreads)
            thread.Join();
    }

    static void RunWavyFromConfig(string configFile)
    {
        try
        {
            var config = File.ReadAllLines(configFile);

            if (config.Length < 3)
            {
                Console.WriteLine($"[ERRO] Configuração inválida no ficheiro: {configFile}");
                return;
            }

            string aggregatorIp = config[0];
            if (!int.TryParse(config[1], out int aggregatorPort))
            {
                Console.WriteLine($"[ERRO] Porta inválida: {config[1]}");
                return;
            }

            string wavyId = config[2];
            string csvFile = config.Length > 3 ? config[3] : "mooloolaba.csv";

            if (!File.Exists(csvFile))
            {
                Console.WriteLine($"[ERRO] Ficheiro CSV não encontrado: {csvFile}");
                return;
            }

            // Verificar e atualizar estado se necessário
            string statusFilePath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\..\\..\\AGREGADOR\\bin\\Debug\\net8.0\\status.txt"));
            if (File.Exists(statusFilePath))
            {
                string[] lines = File.ReadAllLines(statusFilePath);
                for (int i = 0; i < lines.Length; i++)
                {
                    string[] parts = lines[i].Split(':');
                    if (parts.Length >= 4 && parts[0].ToLower() == wavyId.ToLower())
                    {
                        string estadoAtual = parts[1];
                        if (estadoAtual == "associada")
                        {
                            Console.WriteLine($"[WAVY {wavyId}] Estado atual é 'associada'. A alterar para 'operacao' automaticamente...");

                            string novosTipos = "[Hs,Hmax,Tz,Tp,Peak Direction,SST]";
                            string novaLinha = $"{wavyId}:operacao:{novosTipos}:{DateTime.Now:yyyy-MM-dd HH:mm:ss}";
                            lines[i] = novaLinha;
                            File.WriteAllLines(statusFilePath, lines);

                            Console.WriteLine($"[WAVY {wavyId}] Estado alterado para 'operacao'. Pronta para enviar dados.");
                        }
                        break;
                    }
                }
            }

            string progressFile = $"{PROGRESS_PREFIX}{wavyId}.txt";

            // TCP handshake removido: WAVY agora usa apenas RabbitMQ
            // var (ip, port) = SendStartAndGetIp(aggregatorIp, aggregatorPort);
            // if (string.IsNullOrEmpty(ip))
            // {
            //     Console.WriteLine($"[WAVY {wavyId}] Falha ao obter IP do Agregador.");
            //     return;
            // }
            // aggregatorIp = ip;
            // aggregatorPort = port;
            // Console.WriteLine($"[WAVY {wavyId}] Agregador: {aggregatorIp}:{aggregatorPort}");

            // Usar apenas RabbitMQ para transmissão de dados
            StartDataTransmission(aggregatorIp, aggregatorPort, wavyId, csvFile, progressFile);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO WAVY] {ex.Message}");
        }
    }



    static void StartDataTransmission(string aggregatorIp, int aggregatorPort, string wavyId, string csvFile, string progressFile)
    {
        int lastProcessedLine = LoadProgress(progressFile);
        Console.WriteLine($"[WAVY {wavyId}] Última linha processada: {lastProcessedLine}");

        string[] lines = File.ReadAllLines(csvFile);

        // RabbitMQ setup
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();
        channel.ExchangeDeclare(exchange: "wavy_data", type: ExchangeType.Topic);

        string[] tipos = { "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST" };

        for (int i = Math.Max(lastProcessedLine + 1, 1); i < lines.Length; i++)
        {
            string estadoAtual = ObterEstadoAtualDaWavy(wavyId);

            if (estadoAtual == "desativada")
            {
                Console.WriteLine($"[WAVY {wavyId}] Estado é 'desativada'. Envio interrompido.");
                break;
            }

            while (estadoAtual == "manutencao")
            {
                Console.WriteLine($"[WAVY {wavyId}] Em manutenção. Aguardando retoma de operação...");
                Thread.Sleep(2000); // Espera antes de verificar de novo
                estadoAtual = ObterEstadoAtualDaWavy(wavyId);
            }

            if (estadoAtual != "operacao")
            {
                Console.WriteLine($"[WAVY {wavyId}] Estado atual é '{estadoAtual}'. A aguardar...");
                Thread.Sleep(1000);
                i--; // Repetir esta linha depois
                continue;
            }

            string line = lines[i];
            if (!string.IsNullOrWhiteSpace(line))
            {
                try
                {
                    Console.WriteLine($"[WAVY {wavyId}] Publicando linha {i}: {line}");
                    Thread.Sleep(delay);

                    string[] valores = line.Split(',');
                    // Skip the first column (date/time) and start from index 1
                    for (int j = 0; j < tipos.Length && j + 1 < valores.Length; j++)
                    {
                        string topic = tipos[j];
                        string mensagem = valores[j + 1]; // Use j + 1 to skip the date/time column
                        var body = Encoding.UTF8.GetBytes(mensagem);
                        channel.BasicPublish(exchange: "wavy_data",
                                            routingKey: topic,
                                            basicProperties: null,
                                            body: body);
                        Console.WriteLine($"[WAVY {wavyId}] Publicado {mensagem} no tópico {topic}");
                    }
                    SaveProgress(progressFile, i);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WAVY {wavyId}] Erro ao publicar dados: {ex.Message}");
                    break;
                }
            }
        }
        Console.WriteLine($"[WAVY {wavyId}] Transmissão finalizada.");
    }



    static string ObterEstadoAtualDaWavy(string wavyId)
    {
        string statusFilePath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\..\AGREGADOR\bin\Debug\net8.0\status.txt"));
        if (!File.Exists(statusFilePath))
            return "desconhecido";

        var linhas = File.ReadAllLines(statusFilePath);
        foreach (var linha in linhas)
        {
            var partes = linha.Split(':');
            if (partes.Length >= 2 && partes[0].ToLower() == wavyId.ToLower())
            {
                return partes[1].Trim().ToLower();
            }
        }

        return "desconhecido";
    }



    // TCP handshake removido: WAVY agora usa apenas RabbitMQ
    // static (string, int) SendStartAndGetIp(string initialIp, int port)
    // {
    //     string json = JsonSerializer.Serialize(new { type = "START" });
    //     byte[] buffer = Encoding.UTF8.GetBytes(json);
    //
    //     using (TcpClient client = new TcpClient(initialIp, port))
    //     using (NetworkStream stream = client.GetStream())
    //     {
    //         stream.Write(buffer, 0, buffer.Length);
    //
    //         byte[] response = new byte[1024];
    //         int bytesRead = stream.Read(response, 0, response.Length);
    //         string reply = Encoding.UTF8.GetString(response, 0, bytesRead);
    //
    //         var jsonResp = JsonSerializer.Deserialize<JsonElement>(reply);
    //         if (jsonResp.GetProperty("type").GetString() == "ACK")
    //         {
    //             string ip = jsonResp.GetProperty("ip").GetString();
    //             int receivedPort = jsonResp.GetProperty("port").GetInt32();
    //             return (ip, receivedPort);
    //         }
    //     }
    //     return (null, 0);
    // }


    // TCP communication removed: WAVY agora usa apenas RabbitMQ
    // static string SendMessageWithResponse(string serverIp, int port, object payload)
    // {
    //     string json = JsonSerializer.Serialize(payload);
    //     byte[] buffer = Encoding.UTF8.GetBytes(json);
    //
    //     using (TcpClient client = new TcpClient(serverIp, port))
    //     using (NetworkStream stream = client.GetStream())
    //     {
    //         stream.Write(buffer, 0, buffer.Length);
    //
    //         byte[] response = new byte[1024];
    //         int bytesRead = stream.Read(response, 0, response.Length);
    //         return Encoding.UTF8.GetString(response, 0, bytesRead);
    //     }
    // }

    static int LoadProgress(string progressFile)
    {
        if (File.Exists(progressFile))
        {
            string content = File.ReadAllText(progressFile);
            if (int.TryParse(content, out int progress))
            {
                return progress;
            }
        }
        return -1;
    }

    static void SaveProgress(string progressFile, int lineNumber)
    {
        File.WriteAllText(progressFile, lineNumber.ToString());
    }
}