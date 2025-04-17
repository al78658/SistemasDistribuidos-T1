using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

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

        IniciarAgregador(5001, serverIp, 6000);
        IniciarAgregador(5002, serverIp, 6000);
        IniciarAgregador(5003, serverIp, 6001);

        Console.WriteLine("AGREGADOR iniciado e a escutar nas portas 5001, 5002 e 5003.");
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

    static void HandleClient(TcpClient client, string serverIp, int serverPort)
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

    static void SendBufferedData(string ip, int port, string id)
    {
        if (bufferWavy.ContainsKey(id) && bufferWavy[id].Count > 0)
        {
            string conteudo = string.Join(" | ", bufferWavy[id]);
            bufferWavy[id].Clear();

            if (wavyConfigs.ContainsKey(id))
            {
                string preproc = wavyConfigs[id].PreProcessamento;
                conteudo = PreProcessar(conteudo, preproc);
            }

            if (conteudo == null)
            {
                Console.WriteLine($"[ERRO] Conteúdo inválido após pré-processamento para {id}");
                return;
            }

            Console.WriteLine($"[ENVIANDO PARA SERVIDOR] {id}: {conteudo}");

            try
            {
                var payload = new { type = "FORWARD", data = new { id, conteudo } };
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
