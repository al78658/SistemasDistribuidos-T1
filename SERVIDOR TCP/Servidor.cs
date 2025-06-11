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
using AnalysisService.Protos;

namespace SERVIDOR_TCP
{
    class Servidor
    {
        private const int BUFFER_SIZE = 8192;
        private const int PORT1 = 6000;
        private const int PORT2 = 6001;
        private const string ANALYSIS_SERVICE_URL = "http://localhost:7275";

        static readonly SemaphoreSlim ficheiroSemaphore1 = new(1, 1);
        static readonly SemaphoreSlim ficheiroSemaphore2 = new(1, 1);
        static string serverLogFile1 = "dados_servidor1.txt";
        static string serverLogFile2 = "dados_servidor2.txt";

        static DataAnalysis.DataAnalysisClient? analysisClient;
        // Buffer para análise por WAVY/tópico
        static Dictionary<string, List<double>> bufferPorWavyTopico = new();
        // Volume de dados para análise em lote
        static int VolumeAnalise = 5;

        public static async Task Main(string[] args)
        {
            try
            {
                // Initialize gRPC client once
                analysisClient = CriarClienteAnalise();
                await ligarRpc();

                using var listener1 = new TcpListener(IPAddress.Any, PORT1);
                using var listener2 = new TcpListener(IPAddress.Any, PORT2);

                listener1.Start();
                listener2.Start();

                var task1 = Task.Run(() => ListenForClients(listener1, serverLogFile1, ficheiroSemaphore1));
                var task2 = Task.Run(() => ListenForClients(listener2, serverLogFile2, ficheiroSemaphore2));

                // Aguarda que as tarefas terminem (o que nunca acontecerá neste caso, mantendo o servidor em execução)
                await Task.WhenAll(task1, task2);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro fatal no servidor: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static async Task RunMenu()
        {
            string logFile = "dados_servidor1.txt";

            while (true)
            {
                Console.WriteLine("\n--- MENU SERVIDOR ---");
                Console.WriteLine("1. Listar últimos dados recebidos");
                Console.WriteLine("2. Consultar por WAVY, sensor ou intervalo");
                Console.WriteLine("3. Pedir nova análise");
                Console.WriteLine("4. Sair");
                Console.Write("Escolha uma opção: ");
                var op = Console.ReadLine();

                if (op == "1")
                {
                    if (File.Exists(logFile))
                    {
                        var linhas = File.ReadAllLines(logFile).Reverse().Take(20);
                        foreach (var linha in linhas)
                            Console.WriteLine(linha);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum dado encontrado.");
                    }
                }
                else if (op == "2")
                {
                    Console.Write("ID da WAVY (ou Enter para ignorar): ");
                    var wavy = Console.ReadLine();
                    Console.Write("Conteúdo/Sensor (ou Enter para ignorar): ");
                    var sensor = Console.ReadLine();
                    Console.Write("Hora início (HH:mm:ss, ou Enter): ");
                    var inicioStr = Console.ReadLine();
                    Console.Write("Hora fim (HH:mm:ss, ou Enter): ");
                    var fimStr = Console.ReadLine();

                    if (File.Exists(logFile))
                    {
                        var linhas = File.ReadAllLines(logFile);
                        var query = linhas.AsEnumerable();

                        if (!string.IsNullOrWhiteSpace(wavy))
                            query = query.Where(l => l.Contains($"| {wavy} |"));
                        if (!string.IsNullOrWhiteSpace(sensor))
                            query = query.Where(l => l.Contains(sensor));
                        if (TimeSpan.TryParse(inicioStr, out var inicio))
                            query = query.Where(l =>
                            {
                                var partes = l.Split('|');
                                return partes.Length > 0 && TimeSpan.TryParse(partes[0].Trim(), out var t) && t >= inicio;
                            });
                        if (TimeSpan.TryParse(fimStr, out var fim))
                            query = query.Where(l =>
                            {
                                var partes = l.Split('|');
                                return partes.Length > 0 && TimeSpan.TryParse(partes[0].Trim(), out var t) && t <= fim;
                            });

                        foreach (var linha in query.Reverse().Take(50))
                            Console.WriteLine(linha);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum dado encontrado.");
                    }
                }
                else if (op == "3")
                {
                    Console.WriteLine("Parâmetros para nova análise:");
                    Console.Write("ID da WAVY: ");
                    var wavy = Console.ReadLine();
                    Console.Write("Conteúdo/Sensor: ");
                    var sensor = Console.ReadLine();
                    Console.Write("Hora início (HH:mm:ss): ");
                    var inicioStr = Console.ReadLine();
                    Console.Write("Hora fim (HH:mm:ss): ");
                    var fimStr = Console.ReadLine();

                    Console.WriteLine($"[SIMULAÇÃO] Nova análise pedida para WAVY={wavy}, Sensor={sensor}, De={inicioStr} Até={fimStr}");
                    // Aqui podes chamar o teu serviço de análise, se existir
                }
                else if (op == "4")
                {
                    Environment.Exit(0);
                    break;
                }
            }
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
            var channel = GrpcChannel.ForAddress(ANALYSIS_SERVICE_URL, grpcChannelOptions);
            return new DataAnalysis.DataAnalysisClient(channel);
        }

        static async Task ligarRpc()
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

            using var channel = GrpcChannel.ForAddress(ANALYSIS_SERVICE_URL, grpcChannelOptions);
            var client = new SERVIDOR_TCP.Greeter.GreeterClient(channel);

            try
            {
                var reply = await client.SayHelloAsync(new HelloRequest { Name = "SERVIDOR_TCP" });
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao conectar ao servidor gRPC: " + ex.Message);
                throw; // Re-throw to handle in Main
            }
        }

        static async Task ListenForClients(TcpListener listener, string logFile, SemaphoreSlim semaphore)
        {
            while (true)
            {
                try
                {
                    var client = await listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClient(client, logFile, semaphore));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao aceitar cliente: {ex.Message}");
                }
            }
        }

        static async Task HandleClient(TcpClient client, string logFile, SemaphoreSlim semaphore)
        {
            try
            {
                using (client)
                using (NetworkStream stream = client.GetStream())
                {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    using MemoryStream ms = new MemoryStream();
                    int bytesRead;

                    do
                    {
                        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                        if (bytesRead == 0) break; // Connection closed
                        ms.Write(buffer, 0, bytesRead);
                    } while (stream.DataAvailable);

                    string message = Encoding.UTF8.GetString(ms.ToArray());
                    await ProcessMessage(message, logFile, semaphore);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar ligação: {ex.Message}");
            }
        }

        static async Task ProcessMessage(string message, string logFile, SemaphoreSlim semaphore)
        {
            try
            {
                var jsonElement = JsonSerializer.Deserialize<JsonElement>(message);
                
                // Se for um array, processa cada elemento
                if (jsonElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in jsonElement.EnumerateArray())
                    {
                        await ProcessJsonObject(item, logFile, semaphore);
                    }
                }
                // Se for um objeto, processa diretamente
                else if (jsonElement.ValueKind == JsonValueKind.Object)
                {
                    await ProcessJsonObject(jsonElement, logFile, semaphore);
                }
                else
                {
                    Console.WriteLine($"Formato JSON não suportado: {jsonElement.ValueKind}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar mensagem: {ex.Message}");
            }
        }

        static async Task ProcessJsonObject(JsonElement json, string logFile, SemaphoreSlim semaphore)
        {
            try
            {
                // Log do JSON recebido para debug
                Console.WriteLine($"[DEBUG] JSON recebido: {json}");

                // Verifica as propriedades necessárias
                if (!json.TryGetProperty("wavy_id", out var wavyIdElement) ||
                    !json.TryGetProperty("topic", out var topicElement) ||
                    !json.TryGetProperty("timestamp", out var timestampElement) ||
                    !json.TryGetProperty("records", out var recordsElement))
                {
                    Console.WriteLine("[DEBUG] Propriedades obrigatórias não encontradas no JSON");
                    return;
                }

                string wavyId = wavyIdElement.GetString() ?? "unknown";
                string topic = topicElement.GetString() ?? "unknown";
                string timestamp = timestampElement.GetString() ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                
                // Processa os registros: recordsElement já é o array JSON.
                var records = recordsElement;

                if (records.ValueKind != JsonValueKind.Array)
                {
                    Console.WriteLine("[DEBUG] 'records' não é um array válido");
                    return;
                }

                // Lock before iterating and writing to the file
                await semaphore.WaitAsync();
                try
                {
                    // Process each record
                    foreach (var record in records.EnumerateArray())
                    {
                        if (!record.TryGetProperty("data", out var dataElement))
                        {
                            Console.WriteLine("[DEBUG] Propriedade 'data' não encontrada no registro");
                            continue;
                        }

                        string data = dataElement.GetString() ?? "";
                        string recordTimestamp = record.TryGetProperty("timestamp", out var recordTimestampElement) 
                            ? recordTimestampElement.GetString() ?? timestamp 
                            : timestamp;

                        Console.WriteLine($"[RECEBIDO] WAVY: {wavyId}, Tópico: {topic}, Dado: {data}, Timestamp: {recordTimestamp}");

                        // Append to log file inside the lock
                        await File.AppendAllTextAsync(logFile, $"{recordTimestamp} | {wavyId} | {topic} | {data}\n");

                        // Bufferizar para análise em lote
                        string bufferKey = $"{wavyId}_{topic}";
                        if (!bufferPorWavyTopico.ContainsKey(bufferKey))
                            bufferPorWavyTopico[bufferKey] = new List<double>();

                        if (double.TryParse(data, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double valor))
                        {
                            bufferPorWavyTopico[bufferKey].Add(valor);
                        }

                        // Só analisar quando atingir o volume desejado
                        if (bufferPorWavyTopico[bufferKey].Count >= VolumeAnalise && analysisClient != null)
                        {
                            var req = new DataRequest { Source = bufferKey };
                            req.Values.AddRange(bufferPorWavyTopico[bufferKey]);
                            try
                            {
                                var result = await analysisClient.AnalyzeAsync(req);
                                Console.WriteLine($"[ANÁLISE] WAVY: {wavyId}, Tópico: {topic}, Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Erro ao chamar análise RPC: {ex.Message}");
                            }
                            bufferPorWavyTopico[bufferKey].Clear();
                        }
                    }
                }
                finally
                {
                    // Release the lock after the loop is finished
                    semaphore.Release();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar objeto JSON: {ex.Message}");
                Console.WriteLine($"[DEBUG] Stack trace: {ex.StackTrace}");
            }
        }

        private JsonElement CleanAndValidateJson(string jsonString)
        {
            try
            {
                // Parse the input JSON
                var json = JsonDocument.Parse(jsonString).RootElement;
                var cleanedData = new Dictionary<string, object>();

                // Extract timestamp
                if (json.TryGetProperty("timestamp", out var timestampElement))
                {
                    cleanedData["timestamp"] = timestampElement.GetString();
                }

                // Process sensor values
                var sensorKeys = new[] { "Hs", "Hmax", "Tz", "Tp", "Direction", "SST" };
                foreach (var key in sensorKeys)
                {
                    if (json.TryGetProperty(key, out var valueElement))
                    {
                        string value = valueElement.GetString();
                        // Remove any concatenated dates and newlines
                        if (value != null)
                        {
                            value = value.Split('\n')[0].Trim();
                            // Only keep numeric values, ignore sensor names
                            if (double.TryParse(value, out _))
                            {
                                cleanedData[key] = value;
                            }
                        }
                    }
                }

                // Create the properly structured JSON with required properties
                var structuredData = new
                {
                    wavy_id = "wavy01", // Default WAVY ID
                    topic = "sensor_data",
                    timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    records = new[]
                    {
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("Hs") ? cleanedData["Hs"] : "0",
                            sensor_type = "Hs"
                        },
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("Hmax") ? cleanedData["Hmax"] : "0",
                            sensor_type = "Hmax"
                        },
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("Tz") ? cleanedData["Tz"] : "0",
                            sensor_type = "Tz"
                        },
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("Tp") ? cleanedData["Tp"] : "0",
                            sensor_type = "Tp"
                        },
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("Direction") ? cleanedData["Direction"] : "0",
                            sensor_type = "Direction"
                        },
                        new
                        {
                            timestamp = cleanedData.ContainsKey("timestamp") ? cleanedData["timestamp"] : DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                            data = cleanedData.ContainsKey("SST") ? cleanedData["SST"] : "0",
                            sensor_type = "SST"
                        }
                    }
                };

                return JsonDocument.Parse(JsonSerializer.Serialize(structuredData)).RootElement;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Failed to clean and validate JSON: {ex.Message}");
                throw;
            }
        }

        private async Task ProcessJsonData(string jsonString)
        {
            try
            {
                // Clean and validate the JSON first
                var json = CleanAndValidateJson(jsonString);

                // Log the cleaned JSON for debug
                Console.WriteLine($"[DEBUG] Cleaned JSON: {json}");

                // Continue with the existing processing logic
                if (!json.TryGetProperty("wavy_id", out var wavyIdElement) ||
                    !json.TryGetProperty("topic", out var topicElement) ||
                    !json.TryGetProperty("timestamp", out var timestampElement) ||
                    !json.TryGetProperty("records", out var recordsElement))
                {
                    Console.WriteLine("[DEBUG] Propriedades obrigatórias não encontradas no JSON");
                    return;
                }

                string wavyId = wavyIdElement.GetString() ?? "unknown";
                string topic = topicElement.GetString() ?? "unknown";
                string timestamp = timestampElement.GetString() ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                string recordsJson = recordsElement.GetString() ?? "[]";

                // Processa os registros
                var records = JsonSerializer.Deserialize<JsonElement>(recordsJson);
                if (records.ValueKind != JsonValueKind.Array)
                {
                    Console.WriteLine("[DEBUG] 'records' não é um array válido");
                    return;
                }

                // Lock before iterating and writing to the file
                await ficheiroSemaphore1.WaitAsync();
                try
                {
                    // Process each record
                    foreach (var record in records.EnumerateArray())
                    {
                        if (!record.TryGetProperty("data", out var dataElement) ||
                            !record.TryGetProperty("sensor_type", out var sensorTypeElement))
                        {
                            Console.WriteLine("[DEBUG] Propriedades 'data' ou 'sensor_type' não encontradas no registro");
                            continue;
                        }

                        string data = dataElement.GetString() ?? "0";
                        string sensorType = sensorTypeElement.GetString() ?? "unknown";
                        string recordTimestamp = record.TryGetProperty("timestamp", out var recordTimestampElement) 
                            ? recordTimestampElement.GetString() ?? timestamp 
                            : timestamp;

                        Console.WriteLine($"[RECEBIDO] WAVY: {wavyId}, Tópico: {topic}, Sensor: {sensorType}, Dado: {data}, Timestamp: {recordTimestamp}");

                        // Append to log file inside the lock
                        await File.AppendAllTextAsync(serverLogFile1, $"{recordTimestamp} | {wavyId} | {topic} | {sensorType} | {data}\n");

                        // Bufferizar para análise em lote
                        string bufferKey = $"{wavyId}_{topic}_{sensorType}";
                        if (!bufferPorWavyTopico.ContainsKey(bufferKey))
                            bufferPorWavyTopico[bufferKey] = new List<double>();

                        if (double.TryParse(data, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double valor))
                        {
                            bufferPorWavyTopico[bufferKey].Add(valor);
                        }

                        // Só analisar quando atingir o volume desejado
                        if (bufferPorWavyTopico[bufferKey].Count >= VolumeAnalise && analysisClient != null)
                        {
                            var req = new DataRequest { Source = bufferKey };
                            req.Values.AddRange(bufferPorWavyTopico[bufferKey]);
                            try
                            {
                                var result = await analysisClient.AnalyzeAsync(req);
                                Console.WriteLine($"[ANÁLISE] WAVY: {wavyId}, Tópico: {topic}, Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Erro ao chamar análise RPC: {ex.Message}");
                            }
                            bufferPorWavyTopico[bufferKey].Clear();
                        }
                    }
                }
                finally
                {
                    // Release the lock after the loop is finished
                    ficheiroSemaphore1.Release();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao processar dados JSON: {ex.Message}");
                Console.WriteLine($"[DEBUG] Stack trace: {ex.StackTrace}");
            }
        }
    }
}