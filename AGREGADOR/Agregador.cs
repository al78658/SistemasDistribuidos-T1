﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿using System;
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
    
    // Canal RPC global para ser reutilizado
    static GrpcChannel rpcChannel;

    static readonly List<string> ColunasDados = new()
    {
        "Hs", "Hmax", "Tz", "Tp", "Peak Direction", "SST"
    };

    static void Main(string[] args)
    {
        LoadConfigurations();

        // Forçar a configuração da WAVY01 para converter_text_json
        Console.WriteLine("[CONFIG] Forçando configuração da WAVY01 para converter_text_json");
        wavyConfigs["wavy01"] = new WavyConfig
        {
            PreProcessamento = "converter_text_json",
            VolumeDadosEnviar = 5,
            ServidorAssociado = "127.0.0.1",
            FormatoDados = "json",
            TaxaLeitura = "minuto"
        };
        AtualizarConfigWavy("wavy01");

        string serverIp = "127.0.0.1";

        // Alterando as portas para conectar ao Servidor TCP
        IniciarAgregador(7001, serverIp, 6000);
        IniciarAgregador(7002, serverIp, 6000);
        IniciarAgregador(7003, serverIp, 6001);

        Console.WriteLine("AGREGADOR iniciado e a escutar nas portas 7001, 7002 e 7003.");
        Console.WriteLine("Pressiona Ctrl+C para terminar.");

        while (true)
            Thread.Sleep(1000);
    }

    static void IniciarAgregador(int port, string serverIp, int serverPort)
    {
        TcpListener listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"AGREGADOR a escutar na porta {port}");

        Task.Run(() =>
        {
            while (true)
            {
                TcpClient client = listener.AcceptTcpClient();
                Task.Run(() => HandleClient(client, serverIp, serverPort));
            }
        });
        IniciarRcp();
    }

    static async void IniciarRcp()
    {
        try
        {
            var httpHandler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (HttpRequestMessage, cert, chain, sslPolicyErrors) => true
            };

            // Criar um canal global para ser reutilizado
            Console.WriteLine("[RPC] Tentando conectar ao serviço RPC em https://localhost:7177");
            rpcChannel = GrpcChannel.ForAddress("https://localhost:7177", new GrpcChannelOptions
            {
                HttpHandler = httpHandler
            });

            var client = new AGREGADOR.Greeter.GreeterClient(rpcChannel);
            var reply = await client.SayHelloAsync(
                              new HelloRequest { Name = "AGREGADOR" });
            Console.WriteLine("Conexão RPC estabelecida: " + reply.Message);
            
            // Testar a conexão com os novos serviços
            try
            {
                var testReply = await client.ProcessDataAsync(new ProcessDataRequest
                {
                    WavyId = "test",
                    Data = "2023-01-01 12:00:00 10.5 20.3 30.1",
                    SourceFormat = DataFormat.Text,
                    TargetFormat = DataFormat.Text
                });
                
                Console.WriteLine("Serviço de processamento de dados RPC disponível");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Aviso: Serviço de processamento de dados RPC não está disponível: {ex.Message}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Aviso: Não foi possível conectar ao serviço RPC: {ex.Message}");
            Console.WriteLine("O Agregador continuará funcionando com processamento local apenas.");
        }
    }
    static void LoadConfigurations()
    {
        Console.WriteLine("[CONFIG] Carregando configurações...");
        
        if (File.Exists("config_wavy.txt"))
        {
            Console.WriteLine("[CONFIG] Arquivo config_wavy.txt encontrado");
            
            foreach (var line in File.ReadLines("config_wavy.txt"))
            {
                Console.WriteLine($"[CONFIG] Lendo linha: {line}");
                
                var parts = line.Split(':');
                if (parts.Length < 4)
                {
                    Console.WriteLine("[CONFIG] Linha ignorada: formato inválido");
                    continue;
                }

                string wavyId = parts[0].ToLower();
                string preProc = parts[1];
                int volume = int.Parse(parts[2]);
                string servidor = parts[3];
                string formato = parts.Length > 4 ? parts[4] : "text";
                string taxa = parts.Length > 5 ? parts[5] : "minuto";
                
                wavyConfigs[wavyId] = new WavyConfig
                {
                    PreProcessamento = preProc,
                    VolumeDadosEnviar = volume,
                    ServidorAssociado = servidor,
                    FormatoDados = formato,
                    TaxaLeitura = taxa
                };
                
                Console.WriteLine($"[CONFIG] WAVY {wavyId} configurada: PreProc={preProc}, Volume={volume}, Servidor={servidor}, Formato={formato}, Taxa={taxa}");
            }
        }
        else
        {
            Console.WriteLine("[CONFIG] Arquivo config_wavy.txt não encontrado");
        }

        RecarregarStatusWavy();
    }

    static void HandleClient(TcpClient client, string serverIp, int serverPort)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            
            // Increase buffer size and use MemoryStream for larger messages
            byte[] buffer = new byte[8192]; // Increased from 2048 to 8192
            
            // Read all available data from the stream
            using MemoryStream ms = new MemoryStream();
            int bytesRead;
            
            do {
                bytesRead = stream.Read(buffer, 0, buffer.Length);
                ms.Write(buffer, 0, bytesRead);
            } while (stream.DataAvailable);
            
            string message = Encoding.UTF8.GetString(ms.ToArray()).Trim();

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
                            ServidorAssociado = serverIp,
                            FormatoDados = "text",
                            TaxaLeitura = "minuto"
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
                    
                    // Log para depuração
                    Console.WriteLine($"[DEBUG] WAVY {wavyId} - Pré-processamento: {preproc}");
                    if (wavyConfigs.ContainsKey(wavyId))
                    {
                        Console.WriteLine($"[DEBUG] WAVY {wavyId} - Formato: {wavyConfigs[wavyId].FormatoDados}, Taxa: {wavyConfigs[wavyId].TaxaLeitura}");
                    }
                    
                    conteudo = PreProcessar(conteudo, preproc, wavyId);

                    if (conteudo == null)
                    {
                        SendResponse(stream, new { type = "ACK", message = "Dados invalidos descartados." });
                        return;
                    }

                    bufferWavy[wavyId].Add(conteudo);
                    Console.WriteLine($"[BUFFER] {wavyId}: {bufferWavy[wavyId].Count}/{GetVolume(wavyId)} armazenados.");

                    SendResponse(stream, new { type = "ACK", message = "Dados recebidos." });

                    if (bufferWavy[wavyId].Count >= GetVolume(wavyId))
                        SendBufferedData(serverIp, serverPort, wavyId);

                    break;



                case "MAINTENANCE":
                    string maintenanceId = json.GetProperty("id").GetString().ToLower();
                    AtualizarEstadoWavy(maintenanceId, "manutencao");
                    SendResponse(stream, new { type = "ACK", message = $"WAVY {maintenanceId} em manutencao." });
                    break;

                case "END":
                    string endId = json.GetProperty("id").GetString().ToLower();
                    SendBufferedData(serverIp, serverPort, endId);
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
            Console.WriteLine($"[CONFIG] Recarregando configuração para WAVY {wavyId}");
            
            // Forçar a configuração da WAVY01 para converter_text_json
            if (wavyId.ToLower() == "wavy01")
            {
                Console.WriteLine("[CONFIG] Forçando configuração da WAVY01 para converter_text_json");
                wavyConfigs[wavyId] = new WavyConfig
                {
                    PreProcessamento = "converter_text_json",
                    VolumeDadosEnviar = 5,
                    ServidorAssociado = "127.0.0.1",
                    FormatoDados = "json",
                    TaxaLeitura = "minuto"
                };
                return;
            }
            
            if (File.Exists("config_wavy.txt"))
            {
                foreach (var line in File.ReadLines("config_wavy.txt"))
                {
                    Console.WriteLine($"[CONFIG] Lendo linha: {line}");
                    
                    var parts = line.Split(':');
                    if (parts.Length < 4) continue;

                    string id = parts[0].ToLower();
                    if (id == wavyId)
                    {
                        Console.WriteLine($"[CONFIG] Encontrada configuração para WAVY {wavyId}");
                        
                        wavyConfigs[id] = new WavyConfig
                        {
                            PreProcessamento = parts[1],
                            VolumeDadosEnviar = int.Parse(parts[2]),
                            ServidorAssociado = parts[3],
                            FormatoDados = parts.Length > 4 ? parts[4] : "text",
                            TaxaLeitura = parts.Length > 5 ? parts[5] : "minuto"
                        };
                        
                        Console.WriteLine($"[CONFIG] WAVY {id} configurada: PreProc={parts[1]}, Volume={parts[2]}, Servidor={parts[3]}, Formato={(parts.Length > 4 ? parts[4] : "text")}, Taxa={(parts.Length > 5 ? parts[5] : "minuto")}");
                        break;
                    }
                }
            }
            else
            {
                Console.WriteLine("[CONFIG] Arquivo config_wavy.txt não encontrado");
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

    static string PreProcessar(string data, string tipo, string wavyId = "unknown")
    {
        // Verificar se os dados já estão no formato JSON e o tipo é converter_text_json
        if (tipo == "converter_text_json" && 
            ((data.StartsWith("[") && data.EndsWith("]")) || (data.StartsWith("{") && data.EndsWith("}"))))
        {
            try
            {
                // Tentar validar o JSON existente
                JsonDocument.Parse(data);
                Console.WriteLine("[PRÉ-PROCESSAMENTO] Dados já estão em formato JSON válido, pulando conversão");
                return data;
            }
            catch (JsonException)
            {
                // Se não for um JSON válido, continuar com a conversão normal
                Console.WriteLine("[PRÉ-PROCESSAMENTO] Dados parecem ser JSON mas são inválidos, continuando com a conversão");
            }
        }
        
        // Primeiro, aplicamos o pré-processamento local básico
        string processedData = tipo switch
        {
            "trim" => data.Trim(),
            "validar_corrigir" => ValidarECorrigir(data),
            "remover_virgulas" => RemoverVirgulas(data),
            "nenhum" => data,
            _ => data
        };
        
        // Se o tipo de pré-processamento incluir conversão de formato ou padronização de taxa de leitura,
        // usamos o serviço RPC
        if (tipo.Contains("converter_"))
        {
            try
            {
                processedData = ProcessarViaRPC(processedData, tipo, wavyId);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO RPC] Falha ao processar via RPC: {ex.Message}");
                Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                
                // Implementar fallback para converter_text_json
                if (tipo == "converter_text_json")
                {
                    processedData = FormatarDadosParaJson(processedData);
                }
            }
        }
        else if (tipo.Contains("padronizar_"))
        {
            try
            {
                processedData = ProcessarViaRPC(processedData, tipo, wavyId);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO RPC] Falha ao processar via RPC: {ex.Message}");
                // Manter os dados originais
            }
        }
        // Se o tipo for "auto", detectamos automaticamente o formato e aplicamos a conversão adequada
        else if (tipo == "auto" && wavyConfigs.ContainsKey(wavyId))
        {
            var config = wavyConfigs[wavyId];
            
            // Detectar o formato de entrada
            var sourceFormat = DetectarFormatoDados(processedData);
            Console.WriteLine($"[AUTO-DETECÇÃO] Formato detectado para WAVY {wavyId}: {sourceFormat}");
            
            // Se o formato de destino estiver definido e for diferente do formato detectado
            if (!string.IsNullOrEmpty(config.FormatoDados))
            {
                var targetFormat = ParseDataFormat(config.FormatoDados);
                if (sourceFormat != targetFormat)
                {
                    string tipoConversao = $"converter_{sourceFormat.ToString().ToLower()}_{config.FormatoDados}";
                    Console.WriteLine($"[AUTO-CONVERSÃO] Aplicando conversão: {tipoConversao}");
                    processedData = ProcessarViaRPC(processedData, tipoConversao, wavyId);
                }
            }
            
            // Se a taxa de leitura estiver definida, aplicamos a padronização
            if (!string.IsNullOrEmpty(config.TaxaLeitura) && config.TaxaLeitura != "minuto")
            {
                string tipoPadronizacao = $"padronizar_minuto_{config.TaxaLeitura}";
                Console.WriteLine($"[AUTO-PADRONIZAÇÃO] Aplicando padronização: {tipoPadronizacao}");
                processedData = ProcessarViaRPC(processedData, tipoPadronizacao, wavyId);
            }
        }
        // Se não for um tipo especial, mas a WAVY tiver configurações de formato ou taxa,
        // aplicamos o processamento automático
        else if (wavyConfigs.ContainsKey(wavyId))
        {
            var config = wavyConfigs[wavyId];
            
            // Se o formato de dados estiver definido, aplicamos a conversão
            if (!string.IsNullOrEmpty(config.FormatoDados) && config.FormatoDados != "text")
            {
                string tipoConversao = $"converter_text_{config.FormatoDados}";
                Console.WriteLine($"[CONVERSÃO] Aplicando conversão para WAVY {wavyId}: {tipoConversao}");
                processedData = ProcessarViaRPC(processedData, tipoConversao, wavyId);
            }
            
            // Se a taxa de leitura estiver definida, aplicamos a padronização
            if (!string.IsNullOrEmpty(config.TaxaLeitura) && config.TaxaLeitura != "minuto")
            {
                string tipoPadronizacao = $"padronizar_minuto_{config.TaxaLeitura}";
                Console.WriteLine($"[PADRONIZAÇÃO] Aplicando padronização para WAVY {wavyId}: {tipoPadronizacao}");
                processedData = ProcessarViaRPC(processedData, tipoPadronizacao, wavyId);
            }
        }
        
        return processedData;
    }

    static string ProcessarViaRPC(string data, string tipo, string wavyId = "unknown")
    {
        try
        {
            // Verificar se o canal RPC está disponível
            if (rpcChannel == null)
            {
                Console.WriteLine("[ERRO RPC] Canal RPC não inicializado. Usando processamento local.");
                
                // Implementar fallback local para converter_text_json
                if (tipo == "converter_text_json")
                {
                    Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                    return FormatarDadosParaJson(data);
                }
                
                return data;
            }

            var client = new AGREGADOR.Greeter.GreeterClient(rpcChannel);
            
            Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Iniciando para WAVY {wavyId} com tipo: {tipo}");
            Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Dados originais: {data.Substring(0, Math.Min(50, data.Length))}...");
            
            // Determinar o tipo de processamento necessário
            if (tipo.StartsWith("converter_"))
            {
                try
                {
                    // Formato: converter_origem_destino (ex: converter_csv_json)
                    var parts = tipo.Split('_');
                    if (parts.Length < 3)
                    {
                        Console.WriteLine("[ERRO RPC] Formato de tipo inválido. Esperado: converter_origem_destino");
                        return data;
                    }
                    
                    var sourceFormat = ParseDataFormat(parts[1]);
                    var targetFormat = ParseDataFormat(parts[2]);
                    
                    // Implementar fallback local para converter_text_json
                    if (sourceFormat == DataFormat.Text && targetFormat == DataFormat.Json)
                    {
                        try
                        {
                            Console.WriteLine("[PRÉ-PROCESSAMENTO RPC] Tentando usar serviço RPC...");
                            
                            // Criar uma nova requisição para o serviço RPC
                            var request = new ProcessDataRequest
                            {
                                WavyId = wavyId,
                                Data = data,
                                SourceFormat = sourceFormat,
                                TargetFormat = targetFormat
                            };
                            
                            // Chamar o serviço RPC de forma síncrona com timeout
                            var reply = client.ProcessData(request);
                            
                            if (reply.Success)
                            {
                                Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Conversão de formato concluída com sucesso");
                                return reply.ProcessedData;
                            }
                            else
                            {
                                Console.WriteLine($"[ERRO RPC] Falha na conversão de formato: {reply.ErrorMessage}");
                                Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                                return FormatarDadosParaJson(data);
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[ERRO RPC] Exceção durante a conversão de formato: {ex.Message}");
                            Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                            return FormatarDadosParaJson(data);
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[ERRO RPC] Conversão de {sourceFormat} para {targetFormat} não suportada localmente");
                        return data;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO RPC] Exceção durante a conversão de formato: {ex.Message}");
                    
                    // Implementar fallback local para converter_text_json
                    if (tipo == "converter_text_json")
                    {
                        Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                        return FormatarDadosParaJson(data);
                    }
                    
                    return data;
                }
            }
            else if (tipo.StartsWith("padronizar_"))
            {
                try
                {
                    // Formato: padronizar_origem_destino (ex: padronizar_segundo_minuto)
                    var parts = tipo.Split('_');
                    if (parts.Length < 3)
                    {
                        Console.WriteLine("[ERRO RPC] Formato de tipo inválido. Esperado: padronizar_origem_destino");
                        return data;
                    }
                    
                    var sourceInterval = ParseReadingInterval(parts[1]);
                    var targetInterval = ParseReadingInterval(parts[2]);
                    
                    Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Padronizando taxa de leitura de {sourceInterval} para {targetInterval}");
                    
                    // Criar uma nova requisição para o serviço RPC
                    var request = new StandardizeRateRequest
                    {
                        WavyId = wavyId,
                        Data = data,
                        SourceInterval = sourceInterval,
                        TargetInterval = targetInterval,
                        CustomIntervalSeconds = 0 // Valor padrão
                    };
                    
                    Console.WriteLine("[PRÉ-PROCESSAMENTO RPC] Enviando requisição para o serviço RPC...");
                    
                    // Chamar o serviço RPC de forma síncrona
                    var reply = client.StandardizeReadingRate(request);
                    
                    if (reply.Success)
                    {
                        Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Padronização de taxa concluída com sucesso");
                        Console.WriteLine($"[PRÉ-PROCESSAMENTO RPC] Dados padronizados: {reply.StandardizedData.Substring(0, Math.Min(50, reply.StandardizedData.Length))}...");
                        return reply.StandardizedData;
                    }
                    else
                    {
                        Console.WriteLine($"[ERRO RPC] Falha na padronização de taxa: {reply.ErrorMessage}");
                        return data; // Retorna os dados originais em caso de erro
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO RPC] Exceção durante a padronização de taxa: {ex.Message}");
                    Console.WriteLine($"[ERRO RPC] Stack trace: {ex.StackTrace}");
                    return data; // Retorna os dados originais em caso de erro
                }
            }
            
            return data;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO RPC] Exceção geral: {ex.Message}");
            Console.WriteLine($"[ERRO RPC] Stack trace: {ex.StackTrace}");
            
            // Implementar fallback local para converter_text_json
            if (tipo == "converter_text_json")
            {
                Console.WriteLine("[FALLBACK] Usando processamento local para converter_text_json");
                return FormatarDadosParaJson(data);
            }
            
            return data; // Retorna os dados originais em caso de erro
        }
    }
    
    static DataFormat ParseDataFormat(string format)
    {
        return format.ToLower() switch
        {
            "texto" or "text" => DataFormat.Text,
            "csv" => DataFormat.Csv,
            "xml" => DataFormat.Xml,
            "json" => DataFormat.Json,
            _ => DataFormat.Text // Formato padrão
        };
    }
    
    static DataFormat DetectarFormatoDados(string data)
    {
        // Remover espaços em branco no início e fim
        data = data.Trim();
        
        // Verificar se é JSON
        if ((data.StartsWith("{") && data.EndsWith("}")) || 
            (data.StartsWith("[") && data.EndsWith("]")))
        {
            return DataFormat.Json;
        }
        
        // Verificar se é XML
        if (data.StartsWith("<?xml") || 
            (data.StartsWith("<") && data.EndsWith(">")))
        {
            return DataFormat.Xml;
        }
        
        // Verificar se é CSV (verificando se tem vírgulas e linhas)
        if (data.Contains(",") && data.Contains("\n"))
        {
            return DataFormat.Csv;
        }
        
        // Padrão é texto
        return DataFormat.Text;
    }
    
    // Método simplificado para formatar dados de texto para JSON
    static string FormatarDadosParaJson(string data)
    {
        Console.WriteLine("[FALLBACK] Formatando dados para JSON localmente");
        
        // Verificar se os dados já estão em formato JSON
        if ((data.StartsWith("[") && data.EndsWith("]")) || (data.StartsWith("{") && data.EndsWith("}")))
        {
            try
            {
                // Tentar validar o JSON existente
                JsonDocument.Parse(data);
                Console.WriteLine("[FALLBACK] Dados já estão em formato JSON válido");
                return data;
            }
            catch (JsonException)
            {
                // Se não for um JSON válido, continuar com a conversão
                Console.WriteLine("[FALLBACK] Dados parecem ser JSON mas são inválidos, tentando converter");
                
                // Tentar limpar o JSON inválido (remover escapes extras)
                if (data.Contains("\\\""))
                {
                    try
                    {
                        data = data.Replace("\\\"", "\"");
                        JsonDocument.Parse(data);
                        Console.WriteLine("[FALLBACK] JSON corrigido após remover escapes extras");
                        return data;
                    }
                    catch (JsonException)
                    {
                        Console.WriteLine("[FALLBACK] Falha ao corrigir JSON, continuando com a conversão");
                    }
                }
            }
        }
        
        // Verificar se os dados contêm JSON escapado
        if (data.Contains("\\\"timestamp\\\"") || data.Contains("\\\"Hs\\\""))
        {
            try
            {
                // Tentar desescapar o JSON
                data = data.Replace("\\\"", "\"").Replace("\\\\", "\\");
                if ((data.StartsWith("[") && data.EndsWith("]")) || (data.StartsWith("{") && data.EndsWith("}")))
                {
                    JsonDocument.Parse(data);
                    Console.WriteLine("[FALLBACK] JSON corrigido após remover escapes");
                    return data;
                }
            }
            catch (JsonException)
            {
                Console.WriteLine("[FALLBACK] Falha ao corrigir JSON escapado, continuando com a conversão");
            }
        }
        
        // Criar uma lista para armazenar os resultados
        var resultados = new List<Dictionary<string, string>>();
        
        // Dividir os dados em linhas e tratar possíveis delimitadores
        var linhas = data.Split(new[] { '\r', '\n', '|' }, StringSplitOptions.RemoveEmptyEntries);
        
        foreach (var linha in linhas)
        {
            if (string.IsNullOrWhiteSpace(linha))
                continue;
                
            // Dividir a linha em valores
            var valores = linha.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
            
            if (valores.Length < 3)
                continue;
                
            var registro = new Dictionary<string, string>();
            
            // Verificar se os dois primeiros valores formam uma data
            if (DateTime.TryParseExact(
                $"{valores[0]} {valores[1]}", 
                new[] { "yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm" },
                CultureInfo.InvariantCulture, 
                DateTimeStyles.None, 
                out var dataHora))
            {
                // Formato: "2017-07-01 14:30 1.959 3.79 5.492 8.222 95 26.05"
                registro["timestamp"] = dataHora.ToString("yyyy-MM-dd HH:mm");
                
                string[] colunas = { "Hs", "Hmax", "Tz", "Tp", "Direction", "SST" };
                
                for (int i = 0; i < Math.Min(valores.Length - 2, colunas.Length); i++)
                {
                    registro[colunas[i]] = valores[i + 2];
                }
            }
            else
            {
                // Formato alternativo
                registro["timestamp"] = valores[0];
                
                string[] colunas = { "Hs", "Hmax", "Tz", "Tp", "Direction", "SST" };
                
                for (int i = 0; i < Math.Min(valores.Length - 1, colunas.Length); i++)
                {
                    registro[colunas[i]] = valores[i + 1];
                }
            }
            
            resultados.Add(registro);
        }
        
        // Serializar para JSON com opções para evitar escape desnecessário
        var options = new JsonSerializerOptions
        {
            WriteIndented = false,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };
        
        return JsonSerializer.Serialize(resultados, options);
    }
    
    // Método de fallback para converter texto para JSON quando o serviço RPC não está disponível
    static string ConverterTextParaJson(string data)
    {
        // Verificar se os dados já estão em formato JSON
        if ((data.StartsWith("[") && data.EndsWith("]")) || (data.StartsWith("{") && data.EndsWith("}")))
        {
            try
            {
                // Tentar validar o JSON existente
                JsonDocument.Parse(data);
                Console.WriteLine("[CONVERTER] Dados já estão em formato JSON válido");
                return data;
            }
            catch (JsonException)
            {
                // Se não for um JSON válido, continuar com a conversão
                Console.WriteLine("[CONVERTER] Dados parecem ser JSON mas são inválidos, tentando converter");
            }
        }
        
        return FormatarDadosParaJson(data);
    }
    
    static ReadingInterval ParseReadingInterval(string interval)
    {
        return interval.ToLower() switch
        {
            "segundo" or "second" => ReadingInterval.PerSecond,
            "minuto" or "minute" => ReadingInterval.PerMinute,
            "hora" or "hour" => ReadingInterval.PerHour,
            "custom" => ReadingInterval.Custom,
            _ => ReadingInterval.PerMinute // Intervalo padrão
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

    static void SendBufferedData(string ip, int port, string id)
    {
        if (bufferWavy.ContainsKey(id) && bufferWavy[id].Count > 0)
        {
            Console.WriteLine($"[BUFFER] Preparando dados da WAVY {id} para envio ao servidor");
            
            // Tratamento especial para WAVY01 - Solução direta para o problema de dupla serialização
            if (id.ToLower() == "wavy01")
            {
                // Criar dados diretamente no formato correto
                var dadosWavy01 = new List<Dictionary<string, string>>();
                
                // Imprimir o conteúdo do buffer para depuração
                Console.WriteLine("[DEBUG] Conteúdo do buffer WAVY01:");
                foreach (var linha in bufferWavy[id])
                {
                    Console.WriteLine($"[DEBUG] Linha: {linha}");
                    
                    // Verificar se a linha já está em formato JSON
                    if (linha.StartsWith("[") && linha.EndsWith("]"))
                    {
                        try
                        {
                            // Tentar usar o JSON diretamente
                            JsonDocument.Parse(linha);
                            Console.WriteLine("[DEBUG] Linha já é um JSON válido, usando diretamente");
                            
                            // Criar o objeto de mensagem para o servidor
                            var dadosParaEnviarLinha = new { type = "FORWARD", data = new { id, conteudo = linha } };
                            
                            // Serializar a mensagem completa
                            var optionsLinha = new JsonSerializerOptions
                            {
                                WriteIndented = false,
                                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                            };
                            string jsonLinha = JsonSerializer.Serialize(dadosParaEnviarLinha, optionsLinha);
                            
                            // Enviar ao servidor
                            EnviarParaServidor(ip, port, jsonLinha);
                            
                            // Limpar o buffer
                            bufferWavy[id].Clear();
                            return;
                        }
                        catch (JsonException)
                        {
                            Console.WriteLine("[DEBUG] Linha parece ser JSON mas é inválida");
                        }
                    }
                    
                    // Extrair os dados da linha
                    var partes = linha.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    Console.WriteLine($"[DEBUG] Número de partes: {partes.Length}");
                    
                    if (partes.Length >= 8)
                    {
                        var registro = new Dictionary<string, string>
                        {
                            ["timestamp"] = $"{partes[0]} {partes[1]}",
                            ["Hs"] = partes[2],
                            ["Hmax"] = partes[3],
                            ["Tz"] = partes[4],
                            ["Tp"] = partes[5],
                            ["Direction"] = partes[6],
                            ["SST"] = partes[7]
                        };
                        dadosWavy01.Add(registro);
                        Console.WriteLine("[DEBUG] Registro adicionado com sucesso");
                    }
                    else if (partes.Length > 0)
                    {
                        Console.WriteLine("[DEBUG] Linha com formato inesperado, tentando processar");
                        // Tentar processar linhas com formato diferente
                        try
                        {
                            // Verificar se é um formato de texto simples
                            if (partes.Length >= 3 && DateTime.TryParse($"{partes[0]} {partes[1]}", out _))
                            {
                                var registro = new Dictionary<string, string>
                                {
                                    ["timestamp"] = $"{partes[0]} {partes[1]}"
                                };
                                
                                string[] colunas = { "Hs", "Hmax", "Tz", "Tp", "Direction", "SST" };
                                for (int i = 0; i < Math.Min(partes.Length - 2, colunas.Length); i++)
                                {
                                    registro[colunas[i]] = partes[i + 2];
                                }
                                
                                dadosWavy01.Add(registro);
                                Console.WriteLine("[DEBUG] Registro processado com formato alternativo");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[DEBUG] Erro ao processar linha: {ex.Message}");
                        }
                    }
                }
                
                // Verificar se temos dados para enviar
                if (dadosWavy01.Count == 0)
                {
                    Console.WriteLine("[AVISO] Nenhum dado válido encontrado para WAVY01");
                    
                    // Tentar processar o buffer completo como texto
                    string textoCompleto = string.Join(" | ", bufferWavy[id]);
                    Console.WriteLine($"[DEBUG] Tentando processar buffer completo: {textoCompleto.Substring(0, Math.Min(100, textoCompleto.Length))}...");
                    
                    // Aplicar pré-processamento
                    string processado = PreProcessar(textoCompleto, "converter_text_json", id);
                    if (processado != null && processado.StartsWith("[") && processado.EndsWith("]"))
                    {
                        Console.WriteLine("[DEBUG] Pré-processamento gerou JSON válido");
                        
                        // Criar o objeto de mensagem para o servidor
                        var dadosParaEnviarProcessado = new { type = "FORWARD", data = new { id, conteudo = processado } };
                        
                        // Serializar a mensagem completa
                        var optionsProcessado = new JsonSerializerOptions
                        {
                            WriteIndented = false,
                            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                        };
                        string jsonProcessado = JsonSerializer.Serialize(dadosParaEnviarProcessado, optionsProcessado);
                        
                        // Enviar ao servidor
                        EnviarParaServidor(ip, port, jsonProcessado);
                        
                        // Limpar o buffer
                        bufferWavy[id].Clear();
                        return;
                    }
                }
                
                // Serializar diretamente para JSON
                var jsonOptions = new JsonSerializerOptions
                {
                    WriteIndented = false,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                };
                
                string jsonContent = JsonSerializer.Serialize(dadosWavy01, jsonOptions);
                Console.WriteLine($"[BUFFER] Dados formatados diretamente para JSON: {jsonContent.Substring(0, Math.Min(100, jsonContent.Length))}...");
                
                // Criar o objeto de mensagem para o servidor
                var dadosParaEnviarWavy01 = new { type = "FORWARD", data = new { id, conteudo = jsonContent } };
                
                // Serializar a mensagem completa
                string jsonWavy01 = JsonSerializer.Serialize(dadosParaEnviarWavy01, jsonOptions);
                
                // Enviar ao servidor
                EnviarParaServidor(ip, port, jsonWavy01);
                
                // Limpar o buffer
                bufferWavy[id].Clear();
                return;
            }
            
            // Processamento normal para outras WAVYs
            string conteudo = string.Join(" | ", bufferWavy[id]);
            bufferWavy[id].Clear();

            if (wavyConfigs.ContainsKey(id))
            {
                string preproc = wavyConfigs[id].PreProcessamento;
                Console.WriteLine($"[BUFFER] Aplicando pré-processamento '{preproc}' para WAVY {id}");
                
                // Dados antes do pré-processamento
                Console.WriteLine($"[BUFFER] Dados antes do pré-processamento: {conteudo.Substring(0, Math.Min(50, conteudo.Length))}...");
                
                conteudo = PreProcessar(conteudo, preproc, id);
                
                // Dados após o pré-processamento
                Console.WriteLine($"[BUFFER] Dados após pré-processamento: {conteudo?.Substring(0, Math.Min(50, conteudo?.Length ?? 0))}...");
            }

            if (conteudo == null)
            {
                Console.WriteLine($"[ERRO] Conteúdo inválido após pré-processamento para {id}");
                return;
            }

            Console.WriteLine($"[ENVIANDO PARA SERVIDOR] {id}: {conteudo.Substring(0, Math.Min(100, conteudo.Length))}...");

            try
            {
                // Verificar se o conteúdo já é um JSON válido
                string jsonContent = conteudo;
                
                // Se o conteúdo já for um JSON válido e estiver no formato esperado, usá-lo diretamente
                if (jsonContent.StartsWith("[") && jsonContent.EndsWith("]"))
                {
                    try
                    {
                        // Tentar validar o JSON
                        JsonDocument.Parse(jsonContent);
                        Console.WriteLine("[BUFFER] Usando JSON já formatado");
                    }
                    catch (JsonException)
                    {
                        // Se não for JSON válido, converter para o formato esperado
                        jsonContent = FormatarDadosParaJson(conteudo);
                        Console.WriteLine("[BUFFER] Convertendo para JSON");
                    }
                }
                else
                {
                    // Se não for JSON, converter para o formato esperado
                    jsonContent = FormatarDadosParaJson(conteudo);
                    Console.WriteLine("[BUFFER] Convertendo para JSON");
                }
                
                // Criar o objeto de mensagem para o servidor
                var dadosParaEnviarOutros = new { type = "FORWARD", data = new { id, conteudo = jsonContent } };
                
                var optionsOutros = new JsonSerializerOptions
                {
                    WriteIndented = false,
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
                };
                string jsonOutros = JsonSerializer.Serialize(dadosParaEnviarOutros, optionsOutros);
                byte[] buffer = Encoding.UTF8.GetBytes(jsonOutros);
                EnviarParaServidor(ip, port, jsonOutros);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao enviar ao servidor: " + ex.Message);
            }
        }
    }

    static void EnviarParaServidor(string ip, int port, string json)
    {
        try
        {
            byte[] buffer = Encoding.UTF8.GetBytes(json);

            using TcpClient client = new TcpClient(ip, port);
            using NetworkStream stream = client.GetStream();
            
            // Set a larger send buffer size to handle larger payloads
            client.SendBufferSize = 65536; // 64KB buffer size
            
            // Log the size of the data being sent
            Console.WriteLine($"[BUFFER] Enviando {buffer.Length} bytes para o servidor {ip}:{port}");
            
            // Send the data
            stream.Write(buffer, 0, buffer.Length);
            stream.Flush(); // Ensure all data is sent
            
            Console.WriteLine($"[BUFFER] Dados enviados com sucesso para o servidor {ip}:{port}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERRO] Falha ao enviar dados para o servidor: {ex.Message}");
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
            string linha = $"{wavyId}:{config.PreProcessamento}:{config.VolumeDadosEnviar}:{config.ServidorAssociado}:{config.FormatoDados}:{config.TaxaLeitura}";

            Console.WriteLine($"[CONFIG] Atualizando configuração da WAVY {wavyId}: {linha}");

            var linhas = new List<string>();
            if (File.Exists("config_wavy.txt"))
            {
                linhas = new List<string>(File.ReadAllLines("config_wavy.txt"));
                Console.WriteLine($"[CONFIG] Arquivo config_wavy.txt encontrado com {linhas.Count} linhas");
            }
            else
            {
                Console.WriteLine("[CONFIG] Arquivo config_wavy.txt não encontrado, será criado");
            }
            
            bool linhaAtualizada = false;

            for (int i = 0; i < linhas.Count; i++)
            {
                if (linhas[i].StartsWith(wavyId + ":"))
                {
                    Console.WriteLine($"[CONFIG] Atualizando linha existente: {linhas[i]} -> {linha}");
                    linhas[i] = linha;
                    linhaAtualizada = true;
                    break;
                }
            }

            if (!linhaAtualizada)
            {
                Console.WriteLine($"[CONFIG] Adicionando nova linha: {linha}");
                linhas.Add(linha);
            }

            File.WriteAllLines("config_wavy.txt", linhas);
            Console.WriteLine($"[CONFIG] Arquivo config_wavy.txt atualizado com sucesso");
            
            // Recarregar a configuração para garantir que está atualizada em memória
            RecarregarConfiguracaoWavy(wavyId);
        }
        Console.WriteLine($"Configuração da WAVY {wavyId} atualizada no ficheiro config_wavy.txt");
    }
}

class WavyConfig
{
    public string PreProcessamento { get; set; }
    public int VolumeDadosEnviar { get; set; }
    public string ServidorAssociado { get; set; }
    public string FormatoDados { get; set; } = "text"; // Formato padrão: text, csv, xml, json
    public string TaxaLeitura { get; set; } = "minuto"; // Taxa padrão: segundo, minuto, hora, custom
}

class WavyStatus
{
    public string Status { get; set; }
    public List<string> DataTypes { get; set; }
    public DateTime LastSync { get; set; }
}
