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
using SERVIDOR_TCP.Services;
using SERVIDOR_TCP.Data;

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
        static string serverLogFile2 = "dados_servidor2.txt";        static DataAnalysis.DataAnalysisClient? analysisClient;
        static DatabaseService? databaseService;
        static ConfigurationService? configService;
        // Buffer para análise por WAVY/tópico
        static Dictionary<string, List<double>> bufferPorWavyTopico = new();
        // Volume de dados para análise em lote (será carregado da configuração)
        static int VolumeAnalise = 5;        public static void Main(string[] args)
        {
            try
            {
                MainAsync(args).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERRO CRÍTICO: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Console.WriteLine("\n=== DIAGNÓSTICO ===");
                Console.WriteLine("Possíveis causas:");
                Console.WriteLine("1. AnalysisService não está rodando");
                Console.WriteLine("2. Base de dados não pode ser criada");
                Console.WriteLine("3. Portas TCP já estão em uso");
                Console.WriteLine("4. Permissões insuficientes");
                Console.WriteLine("\nPressione qualquer tecla para sair...");
                Console.ReadKey();
            }
        }
        
        static async Task MainAsync(string[] args)
        {
            Console.WriteLine("🌊 SERVIDOR TCP OCEÂNICO - Iniciando...");
            Console.WriteLine("=========================================");
              try
            {
                Console.WriteLine("[INIT] Verificando arquivos de configuração...");
                EnsureAppSettings();
                  Console.WriteLine("[INIT] Inicializando configurações...");
                // Initialize configuration service
                try
                {
                    configService = new ConfigurationService();
                    VolumeAnalise = configService.GetAnalysisVolumeThreshold();
                    Console.WriteLine($"[CONFIGURAÇÃO] ✓ Volume de análise definido para: {VolumeAnalise}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CONFIG] ⚠️ Erro ao carregar configurações: {ex.Message}");
                    Console.WriteLine("[CONFIG] Usando valores padrão...");
                    VolumeAnalise = 3; // Valor padrão
                    configService = null;
                    Console.WriteLine($"[CONFIGURAÇÃO] ✓ Volume de análise padrão: {VolumeAnalise}");
                }
                  Console.WriteLine("[INIT] Inicializando base de dados...");
                // Initialize database service
                try
                {
                    databaseService = new DatabaseService();
                    Console.WriteLine("[SERVIDOR] ✓ Serviço de base de dados inicializado");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BD] ❌ Erro ao inicializar base de dados: {ex.Message}");
                    Console.WriteLine("[BD] O servidor continuará sem persistência de dados.");
                    databaseService = null;
                }
                  
                Console.WriteLine("[INIT] Tentando conectar ao AnalysisService...");
                // Initialize gRPC client once
                try 
                {
                    analysisClient = CriarClienteAnalise();
                    await ligarRpc();
                    Console.WriteLine("[SERVIDOR] ✓ Conexão com AnalysisService estabelecida");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVIDOR] ⚠️ Não foi possível conectar ao AnalysisService: {ex.Message}");
                    Console.WriteLine("[SERVIDOR] O servidor continuará funcionando, mas análises não estarão disponíveis.");
                    Console.WriteLine("[SERVIDOR] Certifique-se de que o AnalysisService está rodando em http://localhost:7275");
                    analysisClient = null;
                }

                Console.WriteLine("[INIT] Iniciando listeners TCP...");
                using var listener1 = new TcpListener(IPAddress.Any, PORT1);
                using var listener2 = new TcpListener(IPAddress.Any, PORT2);

                listener1.Start();
                listener2.Start();
                
                Console.WriteLine($"[SERVIDOR] ✓ Servidor TCP iniciado nas portas {PORT1} e {PORT2}");
                Console.WriteLine("[SERVIDOR] ✓ Sistema pronto para receber dados!");
                Console.WriteLine("=========================================");                var task1 = Task.Run(async () => 
                {
                    try
                    {
                        await ListenForClients(listener1, serverLogFile1, ficheiroSemaphore1);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[TCP1] ❌ Erro no listener porta {PORT1}: {ex.Message}");
                    }
                });
                
                var task2 = Task.Run(async () => 
                {
                    try
                    {
                        await ListenForClients(listener2, serverLogFile2, ficheiroSemaphore2);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[TCP2] ❌ Erro no listener porta {PORT2}: {ex.Message}");
                    }
                });
                
                var menuTask = Task.Run(async () => 
                {
                    try
                    {
                        await RunMenu();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[MENU] ❌ Erro no menu: {ex.Message}");
                    }
                });

                // Aguarda que as tarefas terminem (mas mantém o servidor rodando)
                Console.WriteLine("[SERVIDOR] Pressione Ctrl+C para parar o servidor");
                Console.WriteLine("[SERVIDOR] Ou use a opção 8 no menu para sair");
                
                // Configurar cancelamento via Ctrl+C
                using var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                    Console.WriteLine("\n[SERVIDOR] Shutdown solicitado...");
                };
                
                // Aguardar até ser cancelado
                try
                {
                    await Task.Delay(-1, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("[SERVIDOR] Encerrando servidor...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro fatal no servidor: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                Console.WriteLine("\nPressione qualquer tecla para continuar...");
                Console.ReadKey();
            }
        }        static async Task RunMenu()
        {
            string logFile = "dados_servidor1.txt";
            
            try
            {
                while (true)
                {
                    try
                    {                        Console.WriteLine("\n=== MENU SERVIDOR OCEÂNICO ===");
                        Console.WriteLine("📊 DADOS REAIS:");
                        Console.WriteLine("1. Verificar dados na base de dados (com valores)");
                        Console.WriteLine("2. Reprocessar dados existentes (extrair de rawData)");
                        Console.WriteLine("3. Listar últimos dados recebidos");
                        Console.WriteLine("4. Consultar por WAVY, sensor ou intervalo");
                        Console.WriteLine("");                        Console.WriteLine("🔧 DIAGNÓSTICO:");
                        Console.WriteLine("5. Ver análises realizadas");
                        Console.WriteLine("6. Testar AnalysisService");
                        Console.WriteLine("7. Verificar status da base de dados");
                        Console.WriteLine("8. Analisar dados com parsing detalhado");
                        Console.WriteLine("9. DIAGNÓSTICO COMPLETO DO FLUXO DE DADOS");
                        Console.WriteLine("10. EXAMINAR ESTRUTURA DA BASE DE DADOS");
                        Console.WriteLine("");
                        Console.WriteLine("⚠️  EMERGÊNCIA (só se necessário):");
                        Console.WriteLine("11. Criar dados de teste");
                        Console.WriteLine("0. Sair");
                        Console.Write("Escolha uma opção: ");
                        var op = Console.ReadLine();                if (op == "1")
                {
                    await VerificarBaseDadosDetalhado();
                }
                else if (op == "2")
                {
                    if (databaseService != null)
                    {
                        Console.WriteLine("[MENU] 🔄 Reprocessando dados existentes para extrair valores reais...");
                        await databaseService.ReprocessExistingDataAsync();
                        Console.WriteLine("[MENU] ✅ Reprocessamento concluído! Verifique novamente os dados (opção 1)");
                    }
                    else
                    {
                        Console.WriteLine("[MENU] ❌ Serviço de base de dados não disponível");
                    }
                }
                else if (op == "3")
                {
                    if (File.Exists(logFile))
                    {
                        Console.WriteLine("\n📝 Últimos 20 dados recebidos:");
                        var linhas = File.ReadAllLines(logFile).Reverse().Take(20);
                        foreach (var linha in linhas)
                            Console.WriteLine(linha);
                    }
                    else
                    {
                        Console.WriteLine("Nenhum dado encontrado.");
                    }
                }
                else if (op == "4")
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
                else if (op == "5")
                {
                    await VerificarAnalises();
                }
                else if (op == "6")
                {
                    await TestarAnalysisService();
                }
                else if (op == "7")
                {
                    await VerificarCaminhoBD();
                }
                else if (op == "8")
                {
                    await AnalisarDadosComParsingDetalhado();
                }                else if (op == "9")
                {
                    await DiagnosticarFluxoDados();
                }                else if (op == "10")
                {
                    ExaminarEstruturaBD();
                }
                else if (op == "11")
                {
                    Console.WriteLine("⚠️  Esta opção criará dados FALSOS para teste!");
                    Console.Write("Tem certeza? (s/N): ");
                    var confirmacao = Console.ReadLine();
                    if (confirmacao?.ToLower() == "s")
                    {
                        await CriarDadosTeste();
                        
                        // Também criar dados diretamente na BD para garantir
                        if (databaseService != null)
                        {
                            Console.WriteLine("[TESTE] Criando dados adicionais diretamente na base de dados...");
                            await databaseService.CreateTestDataAsync();
                        }
                    }
                    else
                    {
                        Console.WriteLine("Operação cancelada. Priorize sempre dados reais!");
                    }
                }
                else if (op == "0")
                {
                    Console.WriteLine("[MENU] Saindo do menu. O servidor continuará rodando...");
                    Console.WriteLine("[MENU] Para parar o servidor, use Ctrl+C");
                    break;
                }
                else if (!string.IsNullOrEmpty(op))
                {
                    Console.WriteLine("❌ Opção inválida. Tente novamente.");
                }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Erro no menu: {ex.Message}");
                        Console.WriteLine("Tentando continuar...");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro fatal no menu: {ex.Message}");
                Console.WriteLine("Menu encerrado, mas o servidor continua rodando.");
            }
        }        static DataAnalysis.DataAnalysisClient CriarClienteAnalise()
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
        }static async Task ligarRpc()
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
                Console.WriteLine($"[RPC] Tentando conectar ao AnalysisService em {ANALYSIS_SERVICE_URL}");
                var reply = await client.SayHelloAsync(new HelloRequest { Name = "SERVIDOR_TCP" });
                Console.WriteLine($"[RPC] ✓ Conexão estabelecida com AnalysisService: {reply.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RPC] ❌ Erro ao conectar ao AnalysisService: {ex.Message}");
                Console.WriteLine("[RPC] ⚠️ AnalysisService pode não estar em execução. Inicie o AnalysisService primeiro.");
                throw;
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
        }        static async Task ProcessMessage(string message, string logFile, SemaphoreSlim semaphore)
        {
            try
            {
                Console.WriteLine($"[FLUXO PASSO 5] SERVIDOR: Recebendo dados do AGREGADOR");
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
        }static async Task ProcessJsonObject(JsonElement json, string logFile, SemaphoreSlim semaphore)
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
                string timestamp = timestampElement.GetString() ?? DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");                // Salvar dados na base de dados
                if (databaseService != null)
                {
                    string rawData = json.ToString();
                    bool saved = await databaseService.SaveWavyDataAsync(wavyId, rawData, "aggregated");
                    if (saved)
                    {
                        Console.WriteLine($"[FLUXO PASSO 5] ✓ SERVIDOR→BD: Dados iniciais salvos para WAVY {wavyId} na base de dados");
                    }
                    else
                    {
                        Console.WriteLine($"[FLUXO PASSO 5] ❌ SERVIDOR→BD: Falha ao salvar dados para WAVY {wavyId}");
                    }
                }
                
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
                        await File.AppendAllTextAsync(logFile, $"{recordTimestamp} | {wavyId} | {topic} | {data}\n");                        // Bufferizar para análise em lote
                        string bufferKey = $"{wavyId}_{topic}";
                        if (!bufferPorWavyTopico.ContainsKey(bufferKey))
                            bufferPorWavyTopico[bufferKey] = new List<double>();

                        if (double.TryParse(data, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double valor))
                        {
                            bufferPorWavyTopico[bufferKey].Add(valor);
                            Console.WriteLine($"[BUFFER] {bufferKey}: Adicionado valor {valor:F2}. Total no buffer: {bufferPorWavyTopico[bufferKey].Count}/{VolumeAnalise}");
                        }

                        // Só analisar quando atingir o volume desejado
                        if (bufferPorWavyTopico[bufferKey].Count >= VolumeAnalise && analysisClient != null)
                        {
                            Console.WriteLine($"[FLUXO PASSO 6] SERVIDOR→AnalysisService: Enviando {bufferPorWavyTopico[bufferKey].Count} valores para análise de {bufferKey}");
                            
                            var req = new DataRequest { Source = bufferKey };
                            req.Values.AddRange(bufferPorWavyTopico[bufferKey]);try
                            {
                                var result = await analysisClient.AnalyzeAsync(req);
                                Console.WriteLine($"[ANÁLISE] WAVY: {wavyId}, Tópico: {topic}, Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                                
                                // Salvar resultado na base de dados
                                await SaveAnalysisResultToDatabase(wavyId, topic, result.Mean, result.Stddev, result.Pattern, bufferPorWavyTopico[bufferKey]);
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
                            req.Values.AddRange(bufferPorWavyTopico[bufferKey]);                            try
                            {
                                var result = await analysisClient.AnalyzeAsync(req);
                                Console.WriteLine($"[ANÁLISE] WAVY: {wavyId}, Tópico: {topic}, Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                                
                                // Salvar resultado na base de dados
                                await SaveAnalysisResultToDatabase(wavyId, topic, result.Mean, result.Stddev, result.Pattern, bufferPorWavyTopico[bufferKey]);
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
        }        static async Task SaveAnalysisResultToDatabase(string wavyId, string topic, double mean, double stddev, string pattern, List<double> values)
        {
            if (databaseService != null)
            {
                try
                {
                    string analyzedData = string.Join(",", values);
                    bool saved = await databaseService.SaveAnalysisResultAsync(
                        wavyId, 
                        mean, 
                        stddev, 
                        pattern, 
                        analyzedData, 
                        values.Count
                    );
                    
                    if (saved)
                    {
                        Console.WriteLine($"[FLUXO PASSO 8] ✓ SERVIDOR→BD: Resultado de análise salvo para WAVY {wavyId}, tópico {topic}");
                        Console.WriteLine($"[FLUXO COMPLETO] ✓ Dados agora disponíveis no Dashboard via BD!");
                    }
                    else
                    {
                        Console.WriteLine($"[FLUXO PASSO 8] ❌ SERVIDOR→BD: Falha ao salvar análise para WAVY {wavyId}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BD] ❌ Erro ao salvar análise: {ex.Message}");
                    Console.WriteLine($"[BD] Stack trace: {ex.StackTrace}");
                }
            }
            else
            {
                Console.WriteLine("[BD] ❌ DatabaseService não está disponível!");
            }
        }        static async Task CriarDadosTeste()
        {
            Console.WriteLine("[TESTE] Criando dados de teste para verificar o fluxo completo...");
            
            // Simular dados de múltiplas WAVYs e sensores
            string[] wavyIds = { "WAVY001", "WAVY002", "WAVY003" };
            string[] topics = { "Hs", "Tp", "SST", "Hmax", "Tz" };
            Random random = new Random();
            
            Console.WriteLine($"[TESTE] Volume de análise configurado: {VolumeAnalise}");
            
            for (int ciclo = 0; ciclo < 3; ciclo++) // 3 ciclos para garantir que o volume seja atingido
            {
                foreach (var wavyId in wavyIds)
                {
                    // Criar dados estruturados para cada WAVY com múltiplos registros
                    var testDataStructured = new
                    {
                        wavy_id = wavyId,
                        topic = "sensor_data",
                        timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                        records = topics.Select(topic => new
                        {
                            timestamp = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                            data = topic switch
                            {
                                "Hs" => (random.NextDouble() * 3 + 1).ToString("F2"),
                                "Tp" => (random.NextDouble() * 10 + 5).ToString("F2"),
                                "SST" => (random.NextDouble() * 5 + 18).ToString("F2"),
                                "Hmax" => (random.NextDouble() * 5 + 2).ToString("F2"),
                                "Tz" => (random.NextDouble() * 8 + 4).ToString("F2"),
                                _ => (random.NextDouble() * 100).ToString("F2")
                            },
                            sensor_type = topic
                        }).ToArray()
                    };
                    
                    string jsonData = System.Text.Json.JsonSerializer.Serialize(testDataStructured);
                    Console.WriteLine($"[TESTE] Ciclo {ciclo + 1}/3 - Processando dados para {wavyId}");
                    
                    await ProcessMessage(jsonData, "dados_servidor1.txt", ficheiroSemaphore1);
                    
                    // Pequena pausa entre dados
                    await Task.Delay(300);
                }
                
                Console.WriteLine($"[TESTE] Ciclo {ciclo + 1} concluído. Aguardando processamento...");
                await Task.Delay(1000);
            }
            
            Console.WriteLine("[TESTE] ✓ Dados de teste criados! Verificar:");
            Console.WriteLine("  1. Base de dados para dados brutos (WavyData)");
            Console.WriteLine("  2. Base de dados para análises (AnalysisResults)");
            Console.WriteLine("  3. Dashboard deve mostrar os dados");
            Console.WriteLine($"[TESTE] Total de registros esperados por WAVY/sensor: {3 * topics.Length}");
        }static async Task ProcessarDadosRecebidos(string wavyId, string topic, string data)
        {
            try
            {
                var timestamp = DateTime.UtcNow.ToString("HH:mm:ss");
                string logFile = "dados_servidor1.txt";
                
                Console.WriteLine($"[TESTE-RECEBIDO] WAVY: {wavyId}, Tópico: {topic}, Dado: {data}, Timestamp: {timestamp}");
                
                // Log dos dados recebidos
                await File.AppendAllTextAsync(logFile, $"{timestamp} | {wavyId} | {topic} | {data}\n");
                
                // Salvar na base de dados primeiro
                if (databaseService != null)
                {
                    await databaseService.SaveWavyDataAsync(wavyId, data, "test-generated");
                    Console.WriteLine($"[FLUXO PASSO 5] ✓ SERVIDOR→BD: Dados iniciais salvos na base de dados para WAVY {wavyId}");
                }
                
                // Bufferizar para análise
                string bufferKey = $"{wavyId}_{topic}";
                if (!bufferPorWavyTopico.ContainsKey(bufferKey))
                    bufferPorWavyTopico[bufferKey] = new List<double>();
                
                // Extrair valor numérico dos dados JSON
                if (data.Contains(":"))
                {
                    var parts = data.Split(':');
                    if (parts.Length > 1)
                    {
                        var valueStr = parts[1].Replace("}", "").Replace("\"", "").Split(',')[0].Trim();
                        if (double.TryParse(valueStr, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double valor))
                        {
                            bufferPorWavyTopico[bufferKey].Add(valor);
                        }
                    }
                }
                
                // Analisar quando atingir o volume
                if (bufferPorWavyTopico[bufferKey].Count >= VolumeAnalise && analysisClient != null)
                {
                    Console.WriteLine($"[FLUXO PASSO 6] SERVIDOR→AnalysisService: Enviando valores para análise de {wavyId}, tópico {topic}");
                    
                    var req = new DataRequest { Source = bufferKey };
                    req.Values.AddRange(bufferPorWavyTopico[bufferKey]);
                    
                    try
                    {
                        var result = await analysisClient.AnalyzeAsync(req);
                        Console.WriteLine($"[FLUXO PASSO 7] AnalysisService→SERVIDOR: Análise recebida para {wavyId}, tópico {topic}");
                        Console.WriteLine($"[ANÁLISE] WAVY: {wavyId}, Tópico: {topic}, Média: {result.Mean:F2}, Desvio padrão: {result.Stddev:F2}, Padrão: {result.Pattern}");
                        
                        // Salvar resultado na base de dados
                        await SaveAnalysisResultToDatabase(wavyId, topic, result.Mean, result.Stddev, result.Pattern, bufferPorWavyTopico[bufferKey]);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro ao chamar análise RPC: {ex.Message}");
                    }
                    bufferPorWavyTopico[bufferKey].Clear();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Erro ao processar dados de teste: {ex.Message}");
            }
        }
        
        static async Task VerificarBaseDados()
        {
            if (databaseService != null)
            {
                try
                {
                    Console.WriteLine("[BD-CHECK] Verificando dados na base de dados...");
                    
                    var dados = await databaseService.GetWavyDataAsync(limit: 10);
                    Console.WriteLine($"[BD-CHECK] Total de registros WavyData encontrados: {dados.Count}");
                    
                    if (dados.Any())
                    {
                        Console.WriteLine("[BD-CHECK] Últimos 5 registros:");
                        foreach (var dado in dados.Take(5))
                        {
                            Console.WriteLine($"  WAVY: {dado.WavyId}, Timestamp: {dado.Timestamp:yyyy-MM-dd HH:mm:ss}, Hs: {dado.Hs}, SST: {dado.SST}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("[BD-CHECK] ❌ Nenhum dado encontrado na tabela WavyData!");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BD-CHECK] ❌ Erro ao verificar base de dados: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("[BD-CHECK] ❌ DatabaseService não está disponível!");
            }
        }
        
        static async Task VerificarAnalises()
        {
            if (databaseService != null)
            {
                try
                {
                    Console.WriteLine("[ANÁLISE-CHECK] Verificando análises na base de dados...");
                    
                    var analises = await databaseService.GetAnalysisResultsAsync(limit: 10);
                    Console.WriteLine($"[ANÁLISE-CHECK] Total de análises encontradas: {analises.Count}");
                    
                    if (analises.Any())
                    {
                        Console.WriteLine("[ANÁLISE-CHECK] Últimas 5 análises:");
                        foreach (var analise in analises.Take(5))
                        {
                            Console.WriteLine($"  WAVY: {analise.WavyId}, Timestamp: {analise.AnalysisTimestamp:yyyy-MM-dd HH:mm:ss}, Média: {analise.Mean:F2}, Padrão: {analise.Pattern}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("[ANÁLISE-CHECK] ❌ Nenhuma análise encontrada na tabela AnalysisResults!");
                        Console.WriteLine("[ANÁLISE-CHECK] Sugestão: Execute a opção 6 para criar dados de teste");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ANÁLISE-CHECK] ❌ Erro ao verificar análises: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("[ANÁLISE-CHECK] ❌ DatabaseService não está disponível!");
            }
        }

        static async Task TestarAnalysisService()
        {
            if (analysisClient == null)
            {
                Console.WriteLine("[TESTE-ANÁLISE] ❌ AnalysisService não está conectado!");
                return;
            }
            
            Console.WriteLine("[TESTE-ANÁLISE] Testando conexão com AnalysisService...");
            
            try
            {
                var testRequest = new DataRequest { Source = "test_manual" };
                testRequest.Values.AddRange(new double[] { 1.5, 2.3, 1.8, 2.1, 1.9 });
                
                Console.WriteLine($"[TESTE-ANÁLISE] Enviando dados de teste: [{string.Join(", ", testRequest.Values)}]");
                
                var result = await analysisClient.AnalyzeAsync(testRequest);
                
                Console.WriteLine($"[TESTE-ANÁLISE] ✓ Resultado recebido:");
                Console.WriteLine($"  Média: {result.Mean:F2}");
                Console.WriteLine($"  Desvio Padrão: {result.Stddev:F2}");
                Console.WriteLine($"  Padrão: {result.Pattern}");
                
                // Simular salvamento no banco
                await SaveAnalysisResultToDatabase("TEST_WAVY", "test_topic", result.Mean, result.Stddev, result.Pattern, testRequest.Values.ToList());
                
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TESTE-ANÁLISE] ❌ Erro ao testar AnalysisService: {ex.Message}");
                Console.WriteLine($"[TESTE-ANÁLISE] Stack trace: {ex.StackTrace}");
            }
        }

        static async Task VerificarCaminhoBD()
        {
            var dbPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "SistemasDistribuidos-T1", "oceanic_data.db");
            Console.WriteLine($"[BD-PATH] Caminho da base de dados: {dbPath}");
            Console.WriteLine($"[BD-PATH] Arquivo existe: {File.Exists(dbPath)}");
            
            if (File.Exists(dbPath))
            {
                var fileInfo = new FileInfo(dbPath);
                Console.WriteLine($"[BD-PATH] Tamanho do arquivo: {fileInfo.Length} bytes");
                Console.WriteLine($"[BD-PATH] Última modificação: {fileInfo.LastWriteTime}");
            }
            
            if (databaseService != null)
            {
                try
                {
                    var totalWavy = await databaseService.GetWavyDataAsync(limit: 1000);
                    var totalAnalyses = await databaseService.GetAnalysisResultsAsync(limit: 1000);
                    
                    Console.WriteLine($"[BD-PATH] Total WavyData na BD: {totalWavy.Count}");
                    Console.WriteLine($"[BD-PATH] Total AnalysisResults na BD: {totalAnalyses.Count}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[BD-PATH] ❌ Erro ao consultar BD: {ex.Message}");
                }
            }
        }        static void EnsureAppSettings()
        {
            var currentDir = Directory.GetCurrentDirectory();
            var outputPath = Path.Combine(currentDir, "appsettings.json");
            
            Console.WriteLine($"[CONFIG] Diretório atual: {currentDir}");
            Console.WriteLine($"[CONFIG] Procurando appsettings.json em: {outputPath}");
            
            if (File.Exists(outputPath))
            {
                Console.WriteLine("[CONFIG] ✓ appsettings.json encontrado");
                return;
            }
            
            // Tentar encontrar o arquivo na estrutura do projeto
            var possiblePaths = new[]
            {
                Path.Combine(currentDir, "..", "..", "..", "appsettings.json"),
                Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "appsettings.json"),
                Path.Combine(currentDir, "appsettings.json")
            };
            
            foreach (var sourcePath in possiblePaths)
            {
                if (File.Exists(sourcePath))
                {
                    try
                    {
                        File.Copy(sourcePath, outputPath);
                        Console.WriteLine($"[CONFIG] ✓ appsettings.json copiado de {sourcePath}");
                        return;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[CONFIG] ⚠️ Falha ao copiar de {sourcePath}: {ex.Message}");
                    }
                }
            }
            
            Console.WriteLine("[CONFIG] ⚠️ appsettings.json não encontrado, será usado configuração padrão");        }

        static async Task VerificarBaseDadosDetalhado()
        {
            if (databaseService == null)
            {
                Console.WriteLine("❌ Serviço de base de dados não disponível");
                return;
            }

            try
            {
                Console.WriteLine("\n📊 VERIFICAÇÃO DETALHADA DA BASE DE DADOS");
                Console.WriteLine("==========================================");

                // Contar registros totais
                var totalRecords = await databaseService.GetTotalRecordsAsync();
                Console.WriteLine($"📋 Total de registros: {totalRecords}");

                // Contar registros com valores não nulos
                var recordsWithValues = await databaseService.GetRecordsWithValuesAsync();
                Console.WriteLine($"✅ Registros com valores: {recordsWithValues}");

                // Contar registros só com rawData
                var recordsOnlyRaw = totalRecords - recordsWithValues;
                Console.WriteLine($"⚠️  Registros só com rawData: {recordsOnlyRaw}");

                if (recordsWithValues > 0)
                {
                    Console.WriteLine("\n🎯 DADOS REAIS ENCONTRADOS!");
                    var recentData = await databaseService.GetRecentDataWithValuesAsync(5);
                    
                    if (recentData.Any())
                    {
                        Console.WriteLine("\n📈 Últimos 5 registros com valores reais:");
                        foreach (var data in recentData)
                        {
                            Console.WriteLine($"  {data.Timestamp:yyyy-MM-dd HH:mm:ss} | {data.WavyId} | Hs:{data.Hs:F2} Hmax:{data.Hmax:F2} SST:{data.SST:F1}°C");
                        }
                    }
                }
                else if (recordsOnlyRaw > 0)
                {
                    Console.WriteLine("\n⚠️  ATENÇÃO: Há dados, mas valores estão nulos!");
                    Console.WriteLine("💡 Use a opção 2 do menu para REPROCESSAR e extrair valores reais do rawData");
                    
                    // Mostrar amostra de rawData
                    var sampleRaw = await databaseService.GetSampleRawDataAsync(2);
                    Console.WriteLine("\n📝 Amostra de rawData encontrado:");
                    foreach (var raw in sampleRaw)
                    {
                        Console.WriteLine($"  {raw.Substring(0, Math.Min(100, raw.Length))}...");
                    }
                }
                else
                {
                    Console.WriteLine("\n❌ Nenhum dado encontrado na base de dados");
                    Console.WriteLine("💡 Aguarde que dados sejam recebidos dos sensores WAVY");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro ao verificar base de dados: {ex.Message}");
            }
        }

        static async Task AnalisarDadosComParsingDetalhado()
        {
            if (databaseService == null)
            {
                Console.WriteLine("❌ Serviço de base de dados não disponível");
                return;
            }

            try
            {
                Console.WriteLine("\n🔍 ANÁLISE DETALHADA DO PARSING DE DADOS");
                Console.WriteLine("=========================================");

                // Buscar alguns registros com rawData para análise
                var rawDataSamples = await databaseService.GetRawDataSamplesAsync(3);
                
                if (!rawDataSamples.Any())
                {
                    Console.WriteLine("❌ Nenhum rawData encontrado para análise");
                    return;
                }

                Console.WriteLine($"📊 Analisando {rawDataSamples.Count} amostras de rawData...\n");

                foreach (var (id, rawData) in rawDataSamples.Select((data, index) => (index + 1, data)))
                {
                    Console.WriteLine($"🔍 AMOSTRA {id}:");
                    Console.WriteLine($"Raw data: {rawData.Substring(0, Math.Min(200, rawData.Length))}...");
                    
                    // Tentar fazer parsing e mostrar resultado
                    var parsedValues = databaseService.TestParseDataFromRaw(rawData);
                    
                    if (parsedValues.Any())
                    {
                        Console.WriteLine("✅ Valores extraídos:");
                        foreach (var kv in parsedValues)
                        {
                            Console.WriteLine($"   {kv.Key}: {kv.Value}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("❌ Nenhum valor foi extraído");
                    }
                    
                    Console.WriteLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro na análise: {ex.Message}");
            }
        }

        static async Task DiagnosticarFluxoDados()
        {
            if (databaseService == null)
            {
                Console.WriteLine("❌ Serviço de base de dados não disponível");
                return;
            }

            try
            {
                Console.WriteLine("\n🔍 DIAGNÓSTICO COMPLETO DO FLUXO DE DADOS");
                Console.WriteLine("===========================================");

                // 1. Verificar dados brutos na base
                Console.WriteLine("\n1️⃣ DADOS BRUTOS NA BASE DE DADOS:");
                var rawSamples = await databaseService.GetRawDataSamplesAsync(3);
                
                if (!rawSamples.Any())
                {
                    Console.WriteLine("❌ Nenhum rawData encontrado na base");
                    return;
                }

                for (int i = 0; i < rawSamples.Count; i++)
                {
                    var rawData = rawSamples[i];
                    Console.WriteLine($"\n📋 AMOSTRA {i + 1}:");
                    Console.WriteLine($"Raw Data: {rawData.Substring(0, Math.Min(200, rawData.Length))}...");

                    // 2. Testar parsing individual
                    Console.WriteLine("\n2️⃣ TESTE DE PARSING:");
                    var parsedResult = databaseService.TestParseDataFromRaw(rawData);
                    
                    if (parsedResult.Any())
                    {
                        Console.WriteLine("✅ VALORES EXTRAÍDOS:");
                        foreach (var kv in parsedResult)
                        {
                            Console.WriteLine($"   {kv.Key}: {kv.Value}");
                        }
                    }
                    else
                    {
                        Console.WriteLine("❌ NENHUM VALOR EXTRAÍDO");
                        
                        // Diagnóstico adicional
                        Console.WriteLine("\n🔍 DIAGNÓSTICO DETALHADO:");
                        
                        if (rawData.Contains("\"Hs\"") || rawData.Contains("Hs"))
                        {
                            Console.WriteLine("   ✓ Contém referência a 'Hs'");
                        }
                        if (rawData.Contains("\"records\"") || rawData.Contains("records"))
                        {
                            Console.WriteLine("   ✓ Contém campo 'records'");
                        }
                        if (rawData.Contains("\"data\"") || rawData.Contains("data"))
                        {
                            Console.WriteLine("   ✓ Contém campo 'data'");
                        }
                        if (rawData.Contains("\"sensor_type\"") || rawData.Contains("sensor_type"))
                        {
                            Console.WriteLine("   ✓ Contém campo 'sensor_type'");
                        }
                        
                        // Tentar identificar o formato
                        if (rawData.Trim().StartsWith("{"))
                        {
                            Console.WriteLine("   📄 Formato: JSON Object");
                        }
                        else if (rawData.Trim().StartsWith("["))
                        {
                            Console.WriteLine("   📄 Formato: JSON Array");
                        }
                        else
                        {
                            Console.WriteLine("   📄 Formato: Texto simples");
                        }
                    }

                    Console.WriteLine("\n" + new string('-', 50));
                }

                // 3. Verificar como os dados estão sendo salvos
                Console.WriteLine("\n3️⃣ VERIFICAÇÃO DOS DADOS SALVOS:");
                var recentData = await databaseService.GetRecentDataWithValuesAsync(3);
                
                if (recentData.Any())
                {
                    Console.WriteLine("✅ DADOS COM VALORES ENCONTRADOS:");
                    foreach (var data in recentData)
                    {
                        Console.WriteLine($"   WavyId: {data.WavyId}");
                        Console.WriteLine($"   Timestamp: {data.Timestamp}");
                        Console.WriteLine($"   Hs: {data.Hs} | Hmax: {data.Hmax} | SST: {data.SST}");
                        Console.WriteLine($"   Tp: {data.Tp} | Tz: {data.Tz}");
                        Console.WriteLine();
                    }
                }
                else
                {
                    Console.WriteLine("❌ NENHUM DADO COM VALORES VÁLIDOS ENCONTRADO");
                    Console.WriteLine("💡 Isso significa que o parsing não está funcionando corretamente");
                }

                // 4. Verificar conexão entre servidor e dashboard
                Console.WriteLine("\n4️⃣ CAMINHO SERVIDOR → DASHBOARD:");
                Console.WriteLine("✓ Servidor TCP recebe dados → salva em oceanic_data.db");
                Console.WriteLine("✓ Dashboard lê da mesma oceanic_data.db");
                Console.WriteLine("✓ Se parsing funcionar, Dashboard deve ver os dados");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro durante diagnóstico: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }        static Task ExaminarEstruturaBD()
        {
            if (databaseService == null)
            {
                Console.WriteLine("❌ Serviço de base de dados não disponível");
                return Task.CompletedTask;
            }

            try
            {
                Console.WriteLine("\n🔍 EXAMINANDO ESTRUTURA DA BASE DE DADOS");
                Console.WriteLine("========================================");

                using (var context = new SERVIDOR_TCP.Data.OceanicDataContext())
                {
                    try
                    {
                        // 1. Verificar se a base existe e está acessível
                        Console.WriteLine("1️⃣ Testando conexão à BD...");
                        var testQuery = context.WavyData.Take(1).ToList();
                        Console.WriteLine("✅ Conexão à BD: Sucesso");

                        // 2. Verificar dados na tabela
                        var totalRecords = context.WavyData.Count();
                        Console.WriteLine($"\n2️⃣ TOTAL DE REGISTROS: {totalRecords}");

                        if (totalRecords > 0)
                        {
                            // 3. Análise usando LINQ para Entity Framework
                            var nullHs = context.WavyData.Count(w => w.Hs == null);
                            var nullHmax = context.WavyData.Count(w => w.Hmax == null);
                            var nullSST = context.WavyData.Count(w => w.SST == null);
                            var nullTp = context.WavyData.Count(w => w.Tp == null);
                            var nullTz = context.WavyData.Count(w => w.Tz == null);

                            Console.WriteLine("\n3️⃣ ANÁLISE DE VALORES NULOS:");
                            Console.WriteLine($"   Hs:   {nullHs}/{totalRecords} nulos ({(nullHs * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   Hmax: {nullHmax}/{totalRecords} nulos ({(nullHmax * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   SST:  {nullSST}/{totalRecords} nulos ({(nullSST * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   Tp:   {nullTp}/{totalRecords} nulos ({(nullTp * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   Tz:   {nullTz}/{totalRecords} nulos ({(nullTz * 100.0 / totalRecords):F1}%)");

                            var withValues = context.WavyData.Count(w => w.Hs != null || w.Hmax != null || w.SST != null || w.Tp != null || w.Tz != null);
                            Console.WriteLine($"\n4️⃣ REGISTROS COM PELO MENOS UM VALOR: {withValues}/{totalRecords} ({(withValues * 100.0 / totalRecords):F1}%)");

                            // 4. Mostrar alguns registros de exemplo
                            var sampleRecords = context.WavyData
                                .OrderByDescending(w => w.ReceivedAt)
                                .Take(3)
                                .ToList();

                            Console.WriteLine("\n5️⃣ REGISTROS DE EXEMPLO (3 mais recentes):");
                            foreach (var record in sampleRecords)
                            {
                                Console.WriteLine($"   📄 ID: {record.Id} | WavyId: {record.WavyId ?? "NULL"} | Timestamp: {record.Timestamp}");
                                Console.WriteLine($"      Hs: {record.Hs?.ToString("F2") ?? "NULL"} | Hmax: {record.Hmax?.ToString("F2") ?? "NULL"} | SST: {record.SST?.ToString("F1") ?? "NULL"}");
                                Console.WriteLine($"      Tp: {record.Tp?.ToString("F2") ?? "NULL"} | Tz: {record.Tz?.ToString("F2") ?? "NULL"}");
                                var rawSample = record.RawData?.Length > 100 ? record.RawData.Substring(0, 100) + "..." : record.RawData ?? "NULL";
                                Console.WriteLine($"      RawData: {rawSample}");
                                Console.WriteLine();
                            }

                            // 5. Verificar registros com diferentes combinações de dados
                            var allNullRecords = context.WavyData.Count(w => w.Hs == null && w.Hmax == null && w.SST == null && w.Tp == null && w.Tz == null);
                            var onlyHsRecords = context.WavyData.Count(w => w.Hs != null && w.Hmax == null && w.SST == null && w.Tp == null && w.Tz == null);
                            var onlySSTRecords = context.WavyData.Count(w => w.Hs == null && w.Hmax == null && w.SST != null && w.Tp == null && w.Tz == null);

                            Console.WriteLine("6️⃣ ANÁLISE DETALHADA DOS PADRÕES:");
                            Console.WriteLine($"   Todos campos NULOS: {allNullRecords} ({(allNullRecords * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   Apenas Hs preenchido: {onlyHsRecords} ({(onlyHsRecords * 100.0 / totalRecords):F1}%)");
                            Console.WriteLine($"   Apenas SST preenchido: {onlySSTRecords} ({(onlySSTRecords * 100.0 / totalRecords):F1}%)");

                            // Verificar se há dados com RawData mas sem valores extraídos
                            var withRawDataButNoValues = context.WavyData.Count(w => 
                                w.RawData != null && w.RawData.Length > 0 && 
                                w.Hs == null && w.Hmax == null && w.SST == null && w.Tp == null && w.Tz == null);
                            
                            Console.WriteLine($"   Com RawData mas sem valores extraídos: {withRawDataButNoValues} ({(withRawDataButNoValues * 100.0 / totalRecords):F1}%)");

                            if (withRawDataButNoValues > 0)
                            {
                                Console.WriteLine("\n⚠️ ATENÇÃO: Há registros com dados brutos mas sem valores extraídos!");
                                Console.WriteLine("   Considere reprocessar os dados (opção 2 do menu)");
                            }
                        }
                        else
                        {
                            Console.WriteLine("⚠️ Não há registros na base de dados");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Erro ao conectar à BD: {ex.Message}");
                    }
                }
            }            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro ao examinar estrutura da BD: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            
            return Task.CompletedTask;
        }
    }
}